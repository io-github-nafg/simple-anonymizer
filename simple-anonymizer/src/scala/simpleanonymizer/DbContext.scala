package simpleanonymizer

import org.slf4j.LoggerFactory

import simpleanonymizer.SlickProfile.api._
import slick.jdbc.meta.{MForeignKey, MQName, MTable}
import scala.concurrent.{ExecutionContext, Future}

import slick.jdbc.GetResult

/** A Slick database paired with lazily-cached schema metadata.
  *
  * Wraps a Slick `Database` and schema name, providing lazy accessors for tables, columns, foreign keys, and primary keys. Each accessor triggers a bulk fetch
  * on the first access (for all tables in the schema), then caches the result. This avoids N+1 queries when multiple tables need metadata.
  *
  * Both [[DbCopier]] and [[TableCopier]] can share the same instance. When used with `DbCopier`, the instance is created once and shared. When `TableCopier` is
  * used standalone, it creates its own instance (or accepts one via constructor parameter).
  */
class DbContext(val db: Database, val schema: String)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)

  /** All tables in the schema, sorted by name. */
  lazy val allTables: Future[Seq[MTable]] = {
    logger.info("Fetching tables...")
    db.run(
      MTable.getTables(None, Some(schema), None, Some(Seq("TABLE"))).map(_.sortBy(_.name.name))
    ).map { tables =>
      logger.info(s"Found ${tables.size} tables.")
      tables
    }
  }

  /** All foreign key relationships in the schema. */
  lazy val allForeignKeys: Future[Seq[MForeignKey]] = {
    logger.info("Fetching foreign keys...")
    db.run(
      MForeignKey.getImportedKeys(MQName(None, Some(schema), null))
    ).map { fks =>
      logger.info(s"Found ${fks.size} foreign keys.")
      fks
    }
  }

  /** All column (name, data_type) pairs grouped by table name. Single bulk query serves both column-name and column-type lookups. */
  private lazy val allColumnInfo: Future[Map[String, Map[String, String]]] = {
    logger.info("Fetching columns...")
    db.run(
      sql"""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $schema
      """.as[(String, String, String)]
    ).map(_.groupBy(_._1).map { case (table, cols) => table -> cols.map(c => c._2 -> c._3).toMap })
  }

  /** All column names grouped by table name. */
  lazy val allColumns: Future[Map[String, Seq[String]]] =
    allColumnInfo.map(_.map { case (table, cols) => table -> cols.keys.toSeq })

  /** Column types for a single table. Uses bulk cache, falls back to per-table query if not cached (e.g., dynamically-created tables). */
  def columnTypesFor(tableName: String): Future[Map[String, String]] =
    allColumnInfo.flatMap {
      _.get(tableName) match {
        case Some(typeMap) => Future.successful(typeMap)
        case None          =>
          db.run(
            sql"""
              SELECT column_name, data_type
              FROM information_schema.columns
              WHERE table_schema = $schema AND table_name = $tableName
            """.as[(String, String)]
          ).map(_.toMap)
      }
    }

  /** All sequence-backed columns (SERIAL, BIGSERIAL, GENERATED AS IDENTITY). */
  lazy val allSequences: Future[Seq[DbContext.SequenceInfo]] = {
    logger.info("Fetching sequences...")
    db.run(
      sql"""
        SELECT t.relname, a.attname, s.relname
        FROM pg_class s
        JOIN pg_namespace ns ON ns.oid = s.relnamespace
        JOIN pg_depend d ON d.objid = s.oid
        JOIN pg_class t ON t.oid = d.refobjid
        JOIN pg_namespace nt ON nt.oid = t.relnamespace
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
        WHERE s.relkind = 'S'
          AND ns.nspname = $schema
          AND nt.nspname = $schema
          AND d.deptype IN ('a', 'i')
      """.as[DbContext.SequenceInfo]
    ).map { seqs =>
      logger.info(s"Found ${seqs.size} sequences.")
      seqs
    }
  }

  /** All primary key column names grouped by table name. */
  lazy val allPrimaryKeys: Future[Map[String, Set[String]]] = {
    logger.info("Fetching primary keys...")
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
object DbContext                                                                     {
  case class SequenceInfo(tableName: String, columnName: String, sequenceName: String)
  object SequenceInfo {
    implicit val getSequenceInfo: GetResult[SequenceInfo] = GetResult(r => SequenceInfo(r.<<, r.<<, r.<<))
  }

  private[simpleanonymizer] def fkColumnsByTable(allFks: Seq[MForeignKey]): Map[String, Set[String]] =
    allFks.groupBy(_.fkTable.name).map { case (table, fks) => table -> fks.map(_.fkColumn).toSet }
}
