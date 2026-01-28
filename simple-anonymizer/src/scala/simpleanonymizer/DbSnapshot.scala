package simpleanonymizer

import java.sql.Connection

import scala.collection.mutable

import org.postgresql.util.PGobject

/** Database snapshot and data operations for PostgreSQL.
  *
  * For metadata queries, see [[DbMetadata]].
  */
object DbSnapshot {
  // Re-export ForeignKey for backwards compatibility
  type ForeignKey = DbMetadata.ForeignKey
  val ForeignKey = DbMetadata.ForeignKey

  // Re-export metadata functions for backwards compatibility
  def getForeignKeys(conn: Connection, schema: String = "public"): Seq[ForeignKey] =
    DbMetadata.getForeignKeys(conn, schema)
  def getAllTables(conn: Connection, schema: String = "public"): Seq[String] =
    DbMetadata.getAllTables(conn, schema)
  def getPrimaryKeyColumns(conn: Connection, tableName: String, schema: String = "public"): Set[String] =
    DbMetadata.getPrimaryKeyColumns(conn, tableName, schema)
  def getForeignKeyColumns(conn: Connection, tableName: String, schema: String = "public"): Set[String] =
    DbMetadata.getForeignKeyColumns(conn, tableName, schema)
  def getTableColumns(conn: Connection, tableName: String, schema: String = "public"): Seq[String] =
    DbMetadata.getTableColumns(conn, tableName, schema)
  def validateTransformerCoverage(
    conn: Connection,
    tableName: String,
    transformer: RowTransformer.TableTransformer,
    schema: String = "public"
  ): Either[Set[String], Unit] =
    DbMetadata.validateTransformerCoverage(conn, tableName, transformer, schema)

  // Re-export dependency graph functions for backwards compatibility
  def computeTableLevels(tables: Seq[String], fks: Seq[ForeignKey]): Map[String, Int] =
    DependencyGraph.computeTableLevels(tables, fks)
  def groupTablesByLevel(tableLevels: Map[String, Int]): Seq[Seq[String]] =
    DependencyGraph.groupTablesByLevel(tableLevels)

  /** Configuration for subsetting a table.
    * @param whereClause
    *   Optional WHERE clause to filter rows (e.g., "created_at > '2024-01-01'")
    * @param skip
    *   If true, skip this table entirely
    * @param copyAll
    *   If true, copy all rows (no subsetting based on FK)
    */
  case class TableConfig(
    whereClause: Option[String] = None,
    skip: Boolean = false,
    copyAll: Boolean = false)

  /** Generate a WHERE clause for a child table based on the parent table's filter.
    *
    * For example, if parent = request has filter "created_at > '2024-01-01'" and child = request_field has FK
    * request_field.request_id -> request.id then generates: "request_id IN (SELECT id FROM request WHERE created_at >
    * '2024-01-01')"
    */
  def generateChildWhereClause(
    childTable: String,
    parentFilters: Map[String, String], // parent table -> its WHERE clause
    fks: Seq[ForeignKey]
  ): Option[String] = {
    // Find FKs where this table is the child
    val relevantFks = fks.filter(_.childTable == childTable)

    // For each FK pointing to a parent with a filter, generate a subquery condition
    val conditions = relevantFks.flatMap { fk =>
      parentFilters.get(fk.parentTable).map { parentWhere =>
        s"${fk.childColumn} IN (SELECT ${fk.parentColumn} FROM ${fk.parentTable} WHERE $parentWhere)"
      }
    }

    if (conditions.isEmpty) None
    else Some(conditions.mkString(" AND "))
  }

  /** Compute effective WHERE clauses for all tables based on root filters and FK relationships. Propagates filters from
    * parent tables to child tables through the FK graph.
    *
    * @param tables
    *   All tables in insertion order (by level)
    * @param fks
    *   All foreign key relationships
    * @param tableConfigs
    *   User-specified configurations for tables
    * @return
    *   Map of table -> effective WHERE clause
    */
  def computeEffectiveFilters(
    tables: Seq[String],
    fks: Seq[ForeignKey],
    tableConfigs: Map[String, TableConfig]
  ): Map[String, Option[String]] = {
    val effectiveFilters = mutable.Map[String, Option[String]]()

    // Build reverse lookup: child table -> list of FKs
    val fksByChild = fks.groupBy(_.childTable)

    // Process tables in order (parents before children due to level ordering)
    for (table <- tables) {
      val config = tableConfigs.getOrElse(table, TableConfig())

      if (config.skip) {
        // Skip this table
        effectiveFilters(table) = None
      } else if (config.copyAll) {
        // Copy all rows, no filter
        effectiveFilters(table) = None
      } else if (config.whereClause.isDefined) {
        // User-specified filter takes precedence
        effectiveFilters(table) = config.whereClause
      } else {
        // Auto-generate filter based on parent tables
        val parentFilters = effectiveFilters.collect {
          case (t, Some(filter)) if fksByChild.getOrElse(table, Nil).exists(_.parentTable == t) => t -> filter
        }.toMap

        effectiveFilters(table) = generateChildWhereClause(table, parentFilters, fks)
      }
    }

    effectiveFilters.toMap
  }

  def copyTable(
    sourceConn: Connection,
    targetConn: Connection,
    tableName: String,
    columns: Seq[String],
    whereClause: Option[String] = None,
    limit: Option[Int] = None,
    transformer: Option[RowTransformer.TableTransformer] = None
  ): Int = {
    val columnList   = columns.mkString(", ")
    val placeholders = columns.map(_ => "?").mkString(", ")

    // Add ORDER BY id DESC when using LIMIT for deterministic results (if the table has id column)
    // DESC to get the most recent rows first
    val orderBy = limit.filter(_ => columns.contains("id")).map(_ => " ORDER BY id DESC").getOrElse("")

    val selectSql =
      // language=postgresql
      s"SELECT $columnList FROM $tableName" +
        whereClause.fold("")(w => s" WHERE $w") +
        orderBy +
        limit.fold("")(n => s" LIMIT $n")
    val insertSql =
      // language=postgresql
      s"INSERT INTO $tableName ($columnList) VALUES ($placeholders)"

    println(s"[DbSnapshot] Copying table: $tableName")
    println(s"[DbSnapshot] SELECT: $selectSql")
    transformer.foreach(t => println(s"[DbSnapshot] Transforming columns: ${t.columnNames.mkString(", ")}"))

    val selectStmt = sourceConn.createStatement()
    selectStmt.setFetchSize(1000) // Stream results
    val rs = selectStmt.executeQuery(selectSql)

    val insertStmt = targetConn.prepareStatement(insertSql)
    var count      = 0

    // Get column types for proper JSONB handling
    val rsMetaData  = rs.getMetaData
    val columnTypes = columns.indices.map(i => rsMetaData.getColumnTypeName(i + 1))

    while (rs.next()) {
      // Build row map for transformer
      val rawRow: RowTransformer.Row = columns.map { col =>
        val value = rs.getObject(columns.indexOf(col) + 1)
        col -> (if (value == null) "" else value.toString)
      }.toMap

      // Apply transformation if configured
      val transformedRow = transformer.fold(rawRow)(_.transform(rawRow))

      for (idx <- columns.indices) {
        val column     = columns(idx)
        val columnType = columnTypes(idx)

        // Use transformed value if available, otherwise raw
        val rawValue = rs.getObject(idx + 1)
        val value    =
          if (transformer.exists(_.columnNames.contains(column))) {
            val transformedValue = transformedRow.getOrElse(column, null)
            // Handle JSONB columns - need to wrap in a PGobject
            if ((columnType == "jsonb" || columnType == "json") && transformedValue != null) {
              val jsonObj = new PGobject()
              jsonObj.setType(columnType)
              jsonObj.setValue(transformedValue)
              jsonObj
            } else
              transformedValue
          } else
            rawValue

        insertStmt.setObject(idx + 1, value)
      }
      insertStmt.addBatch()
      count += 1

      if (count % 1000 == 0) {
        insertStmt.executeBatch()
        println(s"[DbSnapshot] Inserted $count rows...")
      }
    }

    // Insert remaining rows
    insertStmt.executeBatch()
    targetConn.commit()

    rs.close()
    selectStmt.close()
    insertStmt.close()

    println(s"[DbSnapshot] Copied $count rows from $tableName")
    count
  }

  def truncateTable(conn: Connection, tableName: String): Unit = {
    println(s"[DbSnapshot] Truncating table: $tableName")
    val stmt = conn.createStatement()
    stmt.execute(s"TRUNCATE TABLE $tableName CASCADE")
    conn.commit()
    stmt.close()
  }
}
