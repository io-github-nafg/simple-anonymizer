package simpleanonymizer

import scala.concurrent.{ExecutionContext, Future}

import org.postgresql.util.PGobject
import slick.jdbc.GetResult
import slick.jdbc.meta.{MForeignKey, MTable}

/** Database snapshot and data operations for PostgreSQL using Slick. */
object DbSnapshot {
  import SlickProfile.api._

  // ============================================================================
  // Metadata queries
  // ============================================================================

  /** Get all foreign key relationships from the database */
  def getForeignKeys(schema: String = "public")(implicit ec: ExecutionContext): DBIO[Seq[MForeignKey]] =
    MTable.getTables(None, Some(schema), None, Some(Seq("TABLE"))).flatMap { tables =>
      DBIO.traverse(tables)(_.getImportedKeys).map(_.flatten)
    }

  /** Get all tables in the specified schema */
  def getAllTables(schema: String = "public")(implicit ec: ExecutionContext): DBIO[Seq[String]] =
    MTable.getTables(None, Some(schema), None, Some(Seq("TABLE"))).map { tables =>
      tables.map(_.name.name).sorted
    }

  /** Helper: get table metadata and apply a function to extract data */
  private def withTable[A](tableName: String, schema: String, default: A)(f: MTable => DBIO[A])(implicit
      ec: ExecutionContext
  ): DBIO[A] =
    MTable.getTables(None, Some(schema), Some(tableName), Some(Seq("TABLE"))).flatMap { tables =>
      tables.headOption match {
        case Some(table) => f(table)
        case None        => DBIO.successful(default)
      }
    }

  /** Get primary key columns for a table */
  def getPrimaryKeyColumns(tableName: String, schema: String = "public")(implicit
      ec: ExecutionContext
  ): DBIO[Set[String]] =
    withTable(tableName, schema, Set.empty[String])(_.getPrimaryKeys.map(_.map(_.column).toSet))

  /** Get foreign key columns for a table (columns that reference other tables) */
  def getForeignKeyColumns(tableName: String, schema: String = "public")(implicit
      ec: ExecutionContext
  ): DBIO[Set[String]] =
    withTable(tableName, schema, Set.empty[String])(_.getImportedKeys.map(_.map(_.fkColumn).toSet))

  /** Get all column names for a table */
  def getTableColumns(tableName: String, schema: String = "public")(implicit ec: ExecutionContext): DBIO[Seq[String]] =
    withTable(tableName, schema, Seq.empty[String])(_.getColumns.map(_.map(_.name)))

  /** Validate that a transformer covers all required columns (excludes PK and FK columns).
    *
    * @return
    *   Left(missing columns) if validation fails, Right(()) if successful
    */
  def validateTransformerCoverage(
      tableName: String,
      transformer: RowTransformer.TableTransformer,
      schema: String = "public"
  )(implicit ec: ExecutionContext): DBIO[Either[Set[String], Unit]] =
    for {
      allColumns <- getTableColumns(tableName, schema)
      pkColumns <- getPrimaryKeyColumns(tableName, schema)
      fkColumns <- getForeignKeyColumns(tableName, schema)
    } yield {
      val requiredColumns = allColumns.toSet -- pkColumns -- fkColumns
      transformer.validateCovers(requiredColumns)
    }

  // ============================================================================
  // Dependency graph
  // ============================================================================

  def computeTableLevels(tables: Seq[String], fks: Seq[MForeignKey]): Map[String, Int] =
    DependencyGraph.computeTableLevels(tables, fks)

  def groupTablesByLevel(tableLevels: Map[String, Int]): Seq[Seq[String]] =
    DependencyGraph.groupTablesByLevel(tableLevels)

  // ============================================================================
  // Filter propagation
  // ============================================================================

  type TableConfig = FilterPropagation.TableConfig
  val TableConfig = FilterPropagation.TableConfig

  def generateChildWhereClause(
      childTable: String,
      parentFilters: Map[String, String],
      fks: Seq[MForeignKey]
  ): Option[String] =
    FilterPropagation.generateChildWhereClause(childTable, parentFilters, fks)

  def computeEffectiveFilters(
      tables: Seq[String],
      fks: Seq[MForeignKey],
      tableConfigs: Map[String, TableConfig]
  ): Map[String, Option[String]] =
    FilterPropagation.computeEffectiveFilters(tables, fks, tableConfigs)

  // ============================================================================
  // Data copy
  // ============================================================================

  /** Wrap a string value as a PGobject for JSON/JSONB columns */
  private def wrapJsonValue(value: String, columnType: String): AnyRef =
    if (value == null) null
    else {
      val jsonObj = new PGobject()
      jsonObj.setType(columnType)
      jsonObj.setValue(value)
      jsonObj
    }

  /** Check if a column type is JSON or JSONB */
  private def isJsonType(columnType: String): Boolean =
    columnType == "jsonb" || columnType == "json"

  /** Get column types for a table using raw SQL */
  private def getColumnTypes(
      tableName: String,
      columns: Seq[String],
      schema: String = "public"
  )(implicit ec: ExecutionContext): DBIO[Seq[String]] = {
    sql"""
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = $schema AND table_name = $tableName
    """.as[(String, String)].map { colTypes =>
      val typeMap = colTypes.toMap
      columns.map(c => typeMap.getOrElse(c, "unknown"))
    }
  }

  // Internal row representation that keeps both raw objects and string values
  private case class RawRow(objects: Map[String, AnyRef], strings: Map[String, String])

  def copyTable(
      sourceDb: Database,
      targetDb: Database,
      tableName: String,
      columns: Seq[String],
      whereClause: Option[String] = None,
      limit: Option[Int] = None,
      transformer: Option[RowTransformer.TableTransformer] = None
  )(implicit ec: ExecutionContext): Future[Int] = {
    val columnList = columns.mkString(", ")
    val placeholders = columns.map(_ => "?").mkString(", ")

    // Add ORDER BY id DESC when using LIMIT for deterministic results (if the table has id column)
    val orderBy = limit.filter(_ => columns.contains("id")).map(_ => " ORDER BY id DESC").getOrElse("")

    val selectSql =
      s"SELECT $columnList FROM $tableName" +
        whereClause.fold("")(w => s" WHERE $w") +
        orderBy +
        limit.fold("")(n => s" LIMIT $n")
    val insertSql = s"INSERT INTO $tableName ($columnList) VALUES ($placeholders)"

    println(s"[DbSnapshot] Copying table: $tableName")
    println(s"[DbSnapshot] SELECT: $selectSql")
    transformer.foreach(t => println(s"[DbSnapshot] Transforming columns: ${t.columnNames.mkString(", ")}"))

    // GetResult for dynamic columns - returns both raw objects and string representations
    implicit val getRowResult: GetResult[RawRow] = GetResult { r =>
      val objectsAndStrings = columns.map { col =>
        val obj = r.nextObject()
        val str = if (obj == null) "" else obj.toString
        (col, obj, str)
      }
      RawRow(
        objects = objectsAndStrings.map { case (col, obj, _) => col -> obj }.toMap,
        strings = objectsAndStrings.map { case (col, _, str) => col -> str }.toMap
      )
    }

    // First get column types from the source database
    val columnTypesAction = getColumnTypes(tableName, columns)

    sourceDb.run(columnTypesAction).flatMap { columnTypes =>
      // Read all rows from source (streaming in batches)
      val selectAction = sql"#$selectSql".as[RawRow]

      sourceDb.run(selectAction).flatMap { rows =>
        if (rows.isEmpty) {
          println(s"[DbSnapshot] No rows to copy from $tableName")
          Future.successful(0)
        } else {
          // Transform and insert in batches
          val batchSize = 1000
          val batches = rows.grouped(batchSize).toList

          // Get set of columns that will be transformed
          val transformedColumns = transformer.map(_.columnNames).getOrElse(Set.empty)

          // Process batches sequentially
          def processBatches(remaining: List[Seq[RawRow]], count: Int): Future[Int] =
            remaining match {
              case Nil           => Future.successful(count)
              case batch :: rest =>
                // Use SimpleDBIO for batch inserts with PreparedStatement
                val batchInsertAction = SimpleDBIO[Int] { ctx =>
                  val conn = ctx.connection
                  val stmt = conn.prepareStatement(insertSql)
                  var batchCount = 0

                  for (rawRow <- batch) {
                    // Apply transformation to string values (only for columns that are being transformed)
                    val transformedStrings = transformer.fold(rawRow.strings)(_.transform(rawRow.strings))

                    for (idx <- columns.indices) {
                      val column = columns(idx)
                      val columnType = columnTypes(idx)

                      val value: AnyRef =
                        if (transformedColumns.contains(column)) {
                          // This column was transformed - use the transformed string value
                          val transformedValue = transformedStrings.getOrElse(column, null)
                          if (isJsonType(columnType)) wrapJsonValue(transformedValue, columnType)
                          else transformedValue
                        } else {
                          // Not transformed - use the original raw object
                          val rawObj = rawRow.objects.getOrElse(column, null)
                          if (rawObj == null) null
                          else if (isJsonType(columnType)) {
                            // For JSON types, we need to wrap in PGobject
                            wrapJsonValue(rawObj.toString, columnType)
                          } else {
                            rawObj
                          }
                        }

                      stmt.setObject(idx + 1, value)
                    }

                    stmt.addBatch()
                    batchCount += 1
                  }

                  stmt.executeBatch()
                  stmt.close()
                  batchCount
                }

                targetDb.run(batchInsertAction).flatMap { inserted =>
                  val newCount = count + inserted
                  if (newCount % 1000 == 0 || rest.isEmpty)
                    println(s"[DbSnapshot] Inserted $newCount rows...")
                  processBatches(rest, newCount)
                }
            }

          processBatches(batches, 0).map { totalCount =>
            println(s"[DbSnapshot] Copied $totalCount rows from $tableName")
            totalCount
          }
        }
      }
    }
  }
}
