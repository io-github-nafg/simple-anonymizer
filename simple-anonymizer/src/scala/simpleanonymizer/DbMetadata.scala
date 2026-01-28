package simpleanonymizer

import java.sql.Connection

import scala.collection.mutable

/** Database metadata query functions for PostgreSQL. */
object DbMetadata {

  /** Foreign key relationship: childTable.childColumn -> parentTable.parentColumn */
  case class ForeignKey(childTable: String, childColumn: String, parentTable: String, parentColumn: String)

  /** Get all foreign key relationships from the database */
  def getForeignKeys(conn: Connection, schema: String = "public"): Seq[ForeignKey] = {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(
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
    val rs = stmt.executeQuery(
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
    val rs = conn.getMetaData.getPrimaryKeys(null, schema, tableName)
    val columns = mutable.Set[String]()
    while (rs.next())
      columns += rs.getString("COLUMN_NAME")
    rs.close()
    columns.toSet
  }

  /** Get foreign key columns for a table (columns that reference other tables) */
  def getForeignKeyColumns(conn: Connection, tableName: String, schema: String = "public"): Set[String] = {
    val rs = conn.getMetaData.getImportedKeys(null, schema, tableName)
    val columns = mutable.Set[String]()
    while (rs.next())
      columns += rs.getString("FKCOLUMN_NAME")
    rs.close()
    columns.toSet
  }

  /** Get all column names for a table */
  def getTableColumns(conn: Connection, tableName: String, schema: String = "public"): Seq[String] = {
    val metaRs = conn.getMetaData.getColumns(null, schema, tableName, null)
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
    val pkColumns = getPrimaryKeyColumns(conn, tableName, schema)
    val fkColumns = getForeignKeyColumns(conn, tableName, schema)

    // Columns that need to be covered: all columns except PK and FK
    val requiredColumns = allColumns -- pkColumns -- fkColumns
    transformer.validateCovers(requiredColumns)
  }
}
