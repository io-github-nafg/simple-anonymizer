package simpleanonymizer

import java.sql.Connection

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

  // Re-export filter propagation types and functions for backwards compatibility
  type TableConfig = FilterPropagation.TableConfig
  val TableConfig = FilterPropagation.TableConfig
  def generateChildWhereClause(
    childTable: String,
    parentFilters: Map[String, String],
    fks: Seq[ForeignKey]
  ): Option[String] =
    FilterPropagation.generateChildWhereClause(childTable, parentFilters, fks)
  def computeEffectiveFilters(
    tables: Seq[String],
    fks: Seq[ForeignKey],
    tableConfigs: Map[String, TableConfig]
  ): Map[String, Option[String]] =
    FilterPropagation.computeEffectiveFilters(tables, fks, tableConfigs)

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
            if (isJsonType(columnType)) wrapJsonValue(transformedValue, columnType)
            else transformedValue
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
