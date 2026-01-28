package simpleanonymizer

import java.sql.Connection

import scala.collection.mutable

import org.postgresql.util.PGobject

object DbSnapshot {

  /** Foreign key relationship: childTable.childColumn -> parentTable.parentColumn */
  case class ForeignKey(childTable: String, childColumn: String, parentTable: String, parentColumn: String)

  /** Get all foreign key relationships from the database */
  def getForeignKeys(conn: Connection, schema: String = "public"): Seq[ForeignKey] = {
    val stmt = conn.createStatement()
    val rs   = stmt.executeQuery(
      // language=postgresql
      s"""SELECT
         |  tc.table_name AS child_table,
         |  kcu.column_name AS child_column,
         |  ccu.table_name AS parent_table,
         |  ccu.column_name AS parent_column
         |FROM information_schema.table_constraints tc
         |JOIN information_schema.key_column_usage kcu
         |  ON tc.constraint_name = kcu.constraint_name
         |  AND tc.table_schema = kcu.table_schema
         |JOIN information_schema.constraint_column_usage ccu
         |  ON ccu.constraint_name = tc.constraint_name
         |  AND ccu.table_schema = tc.table_schema
         |WHERE tc.constraint_type = 'FOREIGN KEY'
         |  AND tc.table_schema = '$schema'""".stripMargin
    )

    val fks = mutable.ArrayBuffer[ForeignKey]()
    while (rs.next())
      fks += ForeignKey(
        childTable = rs.getString("child_table"),
        childColumn = rs.getString("child_column"),
        parentTable = rs.getString("parent_table"),
        parentColumn = rs.getString("parent_column")
      )

    rs.close()
    stmt.close()
    fks.toSeq
  }

  /** Get all tables in the specified schema */
  def getAllTables(conn: Connection, schema: String = "public"): Seq[String] = {
    val stmt = conn.createStatement()
    val rs   = stmt.executeQuery(
      // language=postgresql
      s"""SELECT table_name
         |FROM information_schema.tables
         |WHERE table_schema = '$schema'
         |  AND table_type = 'BASE TABLE'
         |ORDER BY table_name""".stripMargin
    )

    val tables = mutable.ArrayBuffer[String]()
    while (rs.next())
      tables += rs.getString("table_name")

    rs.close()
    stmt.close()
    tables.toSeq
  }

  /** Get primary key columns for a table */
  def getPrimaryKeyColumns(conn: Connection, tableName: String, schema: String = "public"): Set[String] = {
    val rs      = conn.getMetaData.getPrimaryKeys(null, schema, tableName)
    val columns = mutable.Set[String]()
    while (rs.next())
      columns += rs.getString("COLUMN_NAME")
    rs.close()
    columns.toSet
  }

  /** Get foreign key columns for a table (columns that reference other tables) */
  def getForeignKeyColumns(conn: Connection, tableName: String, schema: String = "public"): Set[String] = {
    val rs      = conn.getMetaData.getImportedKeys(null, schema, tableName)
    val columns = mutable.Set[String]()
    while (rs.next())
      columns += rs.getString("FKCOLUMN_NAME")
    rs.close()
    columns.toSet
  }

  /** Get all column names for a table */
  def getTableColumns(conn: Connection, tableName: String, schema: String = "public"): Seq[String] = {
    val metaRs  = conn.getMetaData.getColumns(null, schema, tableName, null)
    val columns = Iterator.continually(metaRs).takeWhile(_.next()).map(_.getString("COLUMN_NAME")).toSeq
    metaRs.close()
    columns
  }

  /** Validate that a transformer covers all required columns (excludes PK and FK columns).
    *
    * @return
    *   Left(missing columns) if validation fails, Right(()) if successful
    */
  def validateTransformerCoverage(
    conn: Connection,
    tableName: String,
    transformer: RowTransformer.TableTransformer,
    schema: String = "public"
  ): Either[Set[String], Unit] = {
    val allColumns = getTableColumns(conn, tableName, schema).toSet
    val pkColumns  = getPrimaryKeyColumns(conn, tableName, schema)
    val fkColumns  = getForeignKeyColumns(conn, tableName, schema)

    // Columns that need to be covered: all columns except PK and FK
    val requiredColumns = allColumns -- pkColumns -- fkColumns
    transformer.validateCovers(requiredColumns)
  }

  /** Compute insertion levels for tables based on FK dependencies. Level 0 = tables with no FK dependencies (can be
    * inserted first) Level N = tables that depend only on tables at level < N
    *
    * Tables at the same level can be inserted in parallel. Returns Map[tableName -> level]
    */
  def computeTableLevels(tables: Seq[String], fks: Seq[ForeignKey]): Map[String, Int] = {
    // Build dependency map: table -> set of tables it depends on (parents)
    val dependencies: Map[String, Set[String]] = {
      val deps = mutable.Map[String, mutable.Set[String]]()
      for (table <- tables)
        deps(table) = mutable.Set.empty
      for (fk <- fks if fk.childTable != fk.parentTable) // Ignore self-references
        deps.get(fk.childTable).foreach(_ += fk.parentTable)
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
      println(s"[DbSnapshot] WARNING: Circular dependencies detected for tables: ${unassigned.mkString(", ")}")

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
