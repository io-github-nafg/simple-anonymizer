package simpleanonymizer

import simpleanonymizer.SlickProfile.api._

import slick.jdbc.meta.{MColumn, MForeignKey, MQName, MTable}

import scala.concurrent.{ExecutionContext, Future}

/** Lazily-caching schema metadata.
  *
  * Each getter triggers a bulk fetch on first access (fetching data for all tables in the schema at once), then caches the result. Subsequent calls return the
  * cached value. This avoids N+1 queries when multiple tables need metadata.
  *
  * Both [[DbCopier]] and [[TableCopier]] can share the same instance. When used with `DbCopier`, the instance is created once and shared. When `TableCopier` is
  * used standalone, it creates its own instance (or accepts one via constructor parameter).
  */
class DbMetadata(db: Database, schema: String)(implicit ec: ExecutionContext) {

  /** All tables in the schema, sorted by name. */
  lazy val allTables: Future[Seq[MTable]] = {
    println("[DbMetadata] Fetching tables...")
    db.run(
      MTable.getTables(None, Some(schema), None, Some(Seq("TABLE"))).map(_.sortBy(_.name.name))
    ).map { tables =>
      println(s"[DbMetadata] Found ${tables.size} tables.")
      tables
    }
  }

  /** All foreign key relationships in the schema. */
  lazy val allForeignKeys: Future[Seq[MForeignKey]] = {
    println("[DbMetadata] Fetching foreign keys...")
    db.run(
      MForeignKey.getImportedKeys(MQName(None, Some(schema), null))
    ).map { fks =>
      println(s"[DbMetadata] Found ${fks.size} foreign keys.")
      fks
    }
  }

  /** All column names grouped by table name. */
  lazy val allColumns: Future[Map[String, Seq[String]]] = {
    println("[DbMetadata] Fetching columns...")
    db.run(
      MColumn.getColumns(MQName(None, Some(schema), "%"), "%").map { columns =>
        columns.groupBy(_.table.name).map { case (table, cols) => table -> cols.map(_.name) }
      }
    )
  }

  /** All primary key column names grouped by table name. */
  lazy val allPrimaryKeys: Future[Map[String, Set[String]]] = {
    println("[DbMetadata] Fetching primary keys...")
    db.run(
      sql"""
        SELECT c.relname, a.attname
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
        WHERE i.indisprimary AND n.nspname = $schema
      """
        .as[(String, String)]
        .map(_.groupBy(_._1).map { case (table, cols) => table -> cols.map(_._2).toSet })
    )
  }
}
