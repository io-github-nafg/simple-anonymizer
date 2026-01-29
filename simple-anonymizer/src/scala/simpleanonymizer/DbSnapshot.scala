package simpleanonymizer

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

import org.postgresql.util.PGobject
import slick.jdbc.GetResult
import slick.jdbc.meta.{MForeignKey, MTable}

/** Database snapshot and data operations for PostgreSQL using Slick. */
object DbSnapshot {
  import SlickProfile.api._
  import SlickProfile.quoteIdentifier

  // ============================================================================
  // Code snippet generation for error messages
  // ============================================================================

  /** Generate a code snippet for a table transformer with passthrough for all columns. */
  def generateTableSnippet(tableName: String, columns: Seq[String]): String = {
    val columnBindings = columns.map(col => s""""$col" -> passthrough""").mkString(",\n    ")
    s""""$tableName" -> table(\n    $columnBindings\n  )"""
  }

  /** Generate code snippets for missing column bindings. */
  def generateColumnSnippets(columns: Set[String]): String =
    columns.toSeq.sorted.map(col => s""""$col" -> passthrough""").mkString(",\n    ")

  /** List columns that need transformers (non-PK, non-FK columns). */
  def getDataColumns(tableName: String, schema: String = "public")(implicit ec: ExecutionContext): DBIO[Seq[String]] =
    for {
      columns   <- getTableColumns(tableName, schema)
      pkColumns <- getPrimaryKeyColumns(tableName, schema)
      fkColumns <- getForeignKeyColumns(tableName, schema)
    } yield columns.filterNot(c => pkColumns.contains(c) || fkColumns.contains(c))

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

  /** Validate that a transformer covers all required columns (excludes PK and FK columns). */
  def validateTransformerCoverage(
      tableName: String,
      transformer: RowTransformer.TableTransformer,
      schema: String = "public"
  )(implicit ec: ExecutionContext): DBIO[Either[Set[String], Unit]] =
    for {
      allColumns <- getTableColumns(tableName, schema)
      pkColumns  <- getPrimaryKeyColumns(tableName, schema)
      fkColumns  <- getForeignKeyColumns(tableName, schema)
    } yield {
      val requiredColumns = allColumns.toSet -- pkColumns -- fkColumns
      transformer.validateCovers(requiredColumns)
    }

  // ============================================================================
  // Dependency graph (inlined from DependencyGraph)
  // ============================================================================

  /** Compute insertion levels for tables based on FK dependencies. Level 0 = tables with no FK dependencies (can be inserted first) Level N = tables that
    * depend only on tables at level < N
    */
  def computeTableLevels(tables: Seq[String], fks: Seq[MForeignKey]): Map[String, Int] = {
    // Build dependency map: table -> set of tables it depends on (parents)
    val dependencies: Map[String, Set[String]] = {
      val deps = mutable.Map[String, mutable.Set[String]]()
      for (table <- tables)
        deps(table) = mutable.Set.empty
      for (fk    <- fks if fk.fkTable.name != fk.pkTable.name) // Ignore self-references
        deps.get(fk.fkTable.name).foreach(_ += fk.pkTable.name)
      deps.view.mapValues(_.toSet).toMap
    }

    val levels  = mutable.Map[String, Int]()
    var changed = true

    // Initialize: tables with no dependencies are level 0
    for (table <- tables if dependencies(table).isEmpty)
      levels(table) = 0

    // Iterate until stable
    while (changed) {
      changed = false
      for (table <- tables if !levels.contains(table)) {
        val deps = dependencies(table)
        if (deps.forall(levels.contains)) {
          levels(table) = deps.map(levels).max + 1
          changed = true
        }
      }
    }

    // Check for cycles (tables not assigned a level)
    val unassigned = tables.filterNot(levels.contains)
    if (unassigned.nonEmpty)
      println(s"[DependencyGraph] WARNING: Circular dependencies detected for tables: ${unassigned.mkString(", ")}")

    levels.toMap
  }

  /** Group tables by their level */
  def groupTablesByLevel(tableLevels: Map[String, Int]): Seq[Seq[String]] = {
    if (tableLevels.isEmpty) return Seq.empty
    val maxLevel = tableLevels.values.max
    (0 to maxLevel).map { level =>
      tableLevels.collect { case (table, l) if l == level => table }.toSeq.sorted
    }
  }

  // ============================================================================
  // Filter propagation (inlined from FilterPropagation)
  // ============================================================================

  /** Configuration for subsetting a table. */
  case class TableConfig(whereClause: Option[String] = None, skip: Boolean = false)

  /** Specification for how to handle a table during snapshot copy. */
  sealed trait TableSpec {
    def transformer: Option[RowTransformer.TableTransformer]
    def config: TableConfig
  }
  object TableSpec       {

    /** Skip this table entirely */
    private case object Skip extends TableSpec {
      def transformer: Option[RowTransformer.TableTransformer] = None
      def config: TableConfig                                  = TableConfig(skip = true)
    }

    /** Copy with the given transformer */
    private case class Copy(
        t: RowTransformer.TableTransformer,
        whereClause: Option[String] = None
    ) extends TableSpec {
      def transformer: Option[RowTransformer.TableTransformer] = Some(t)
      def config: TableConfig                                  = TableConfig(whereClause = whereClause)
    }

    /** Skip this table entirely */
    def skip: TableSpec = Skip

    /** Copy with the given transformer */
    def copy(transformer: RowTransformer.TableTransformer): TableSpec = Copy(transformer)

    /** Copy with the given transformer and where clause filter */
    def copy(transformer: RowTransformer.TableTransformer, where: String): TableSpec =
      Copy(transformer, whereClause = Some(where))
  }

  /** Generate a WHERE clause for a child table based on the parent table's filter. */
  def generateChildWhereClause(
      childTable: String,
      parentFilters: Map[String, String],
      fks: Seq[MForeignKey]
  ): Option[String] = {
    val relevantFks = fks.filter(_.fkTable.name == childTable)
    val conditions  = relevantFks.flatMap { fk =>
      parentFilters.get(fk.pkTable.name).map { parentWhere =>
        s"${fk.fkColumn} IN (SELECT ${fk.pkColumn} FROM ${fk.pkTable.name} WHERE $parentWhere)"
      }
    }
    if (conditions.isEmpty) None
    else Some(conditions.mkString(" AND "))
  }

  /** Compute effective WHERE clauses for all tables based on root filters and FK relationships. */
  def computeEffectiveFilters(
      tables: Seq[String],
      fks: Seq[MForeignKey],
      tableConfigs: Map[String, TableConfig]
  ): Map[String, Option[String]] = {
    val effectiveFilters = mutable.Map[String, Option[String]]()
    val fksByChild       = fks.groupBy(_.fkTable.name)

    for (table <- tables) {
      val config = tableConfigs.getOrElse(table, TableConfig())

      if (config.skip)
        effectiveFilters(table) = None
      else if (config.whereClause.isDefined)
        effectiveFilters(table) = config.whereClause
      else {
        val parentFilters = effectiveFilters.collect {
          case (t, Some(filter)) if fksByChild.getOrElse(table, Nil).exists(_.pkTable.name == t) => t -> filter
        }.toMap

        effectiveFilters(table) = generateChildWhereClause(table, parentFilters, fks)
      }
    }

    effectiveFilters.toMap
  }

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
  )(implicit ec: ExecutionContext): DBIO[Seq[String]] =
    sql"""
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = $schema AND table_name = $tableName
    """.as[(String, String)].map { colTypes =>
      val typeMap = colTypes.toMap
      columns.map(c => typeMap.getOrElse(c, "unknown"))
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
    val quotedTable  = quoteIdentifier(tableName)
    val columnList   = columns.map(quoteIdentifier).mkString(", ")
    val placeholders = columns.map(_ => "?").mkString(", ")

    val orderBy = limit.filter(_ => columns.contains("id")).map(_ => s" ORDER BY ${quoteIdentifier("id")} DESC").getOrElse("")

    val selectSql =
      s"SELECT $columnList FROM $quotedTable" +
        whereClause.fold("")(w => s" WHERE $w") +
        orderBy +
        limit.fold("")(n => s" LIMIT $n")
    val insertSql = s"INSERT INTO $quotedTable ($columnList) VALUES ($placeholders)"

    println(s"[DbSnapshot] Copying table: $tableName")
    println(s"[DbSnapshot] SELECT: $selectSql")
    transformer.foreach(t => println(s"[DbSnapshot] Transforming columns: ${t.columnNames.mkString(", ")}"))

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

    val columnTypesAction = getColumnTypes(tableName, columns)

    sourceDb.run(columnTypesAction).flatMap { columnTypes =>
      val selectAction = sql"#$selectSql".as[RawRow]

      sourceDb.run(selectAction).flatMap { rows =>
        if (rows.isEmpty) {
          println(s"[DbSnapshot] No rows to copy from $tableName")
          Future.successful(0)
        } else {
          val batchSize = 1000
          val batches   = rows.grouped(batchSize).toList

          // Build a map of column name -> ColumnPlan for a quick lookup
          val columnPlans: Map[String, RowTransformer.ColumnPlan] =
            transformer.map(_.columns.map(plan => plan.columnName -> plan).toMap).getOrElse(Map.empty)

          def processBatches(remaining: List[Seq[RawRow]], count: Int): Future[Int] =
            remaining match {
              case Nil           => Future.successful(count)
              case batch :: rest =>
                val batchInsertAction = SimpleDBIO[Int] { ctx =>
                  val conn       = ctx.connection
                  val stmt       = conn.prepareStatement(insertSql)
                  var batchCount = 0

                  for (rawRow <- batch) {
                    for (idx <- columns.indices) {
                      val column     = columns(idx)
                      val columnType = columnTypes(idx)

                      val value: AnyRef = columnPlans.get(column) match {
                        case Some(plan) =>
                          plan match {
                            case _: RowTransformer.ColumnPlan.Passthrough =>
                              val rawObj = rawRow.objects.getOrElse(column, null)
                              if (rawObj == null) null
                              else if (isJsonType(columnType)) wrapJsonValue(rawObj.toString, columnType)
                              else rawObj

                            case _: RowTransformer.ColumnPlan.SetNull =>
                              null

                            case fixed: RowTransformer.ColumnPlan.Fixed =>
                              val v = fixed.value
                              if (v == null) null
                              else if (isJsonType(columnType)) wrapJsonValue(v.toString, columnType)
                              else v.asInstanceOf[AnyRef]

                            case transform: RowTransformer.ColumnPlan.Transform =>
                              val str         = rawRow.strings.getOrElse(column, "")
                              val transformed = transform.f(str)
                              if (isJsonType(columnType)) wrapJsonValue(transformed, columnType)
                              else transformed

                            case jsonTransform: RowTransformer.ColumnPlan.TransformJson =>
                              val str         = rawRow.strings.getOrElse(column, "")
                              val transformed = jsonTransform.nav.wrap(jsonTransform.f)(str)
                              if (isJsonType(columnType)) wrapJsonValue(transformed, columnType)
                              else transformed

                            case dep: RowTransformer.ColumnPlan.Dependent =>
                              val str         = rawRow.strings.getOrElse(column, "")
                              val f           = dep.f(rawRow.strings)
                              val transformed = f(str)
                              if (isJsonType(columnType)) wrapJsonValue(transformed, columnType)
                              else transformed
                          }

                        case None =>
                          // Column not in transformer - use original raw object
                          val rawObj = rawRow.objects.getOrElse(column, null)
                          if (rawObj == null) null
                          else if (isJsonType(columnType)) wrapJsonValue(rawObj.toString, columnType)
                          else rawObj
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
