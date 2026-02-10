package simpleanonymizer

import java.sql.Connection

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import slick.dbio.SynchronousDatabaseAction
import slick.jdbc.{GetResult, JdbcBackend}
import slick.util.DumpInfo

import org.postgresql.util.PGobject
import simpleanonymizer.SlickProfile.api._
import simpleanonymizer.SlickProfile.quoteIdentifier

/** Synchronous DBIO action that streams rows from a source database and batch-inserts them into the target via a pinned JDBC connection.
  *
  * Runs within a Slick transaction on the target database. Reads are streamed from `sourceDb` using a server-side cursor (via `fetchSize`), buffered into
  * batches by [[CopyAction.BatchInserter]], and inserted using JDBC prepared statements on the target connection provided by the action context.
  *
  * @param sourceDb
  *   Database to stream rows from.
  * @param columns
  *   Ordered (output column, database type) pairs — used for both SELECT column ordering and INSERT parameter binding with JSON wrapping.
  */
private[simpleanonymizer] class CopyAction(
    dbContext: DbContext,
    tableName: String,
    tableSpec: TableSpec,
    columns: Seq[(OutputColumn, String)]
)(implicit executionContext: ExecutionContext)
    extends SynchronousDatabaseAction[Int, NoStream, JdbcBackend#JdbcActionContext, JdbcBackend#JdbcStreamingActionContext, Effect.All] {

  override type StreamState = Null

  private val quotedTableName = quoteIdentifier(tableName)

  override def getDumpInfo = DumpInfo(name = "CopyAction", mainInfo = quotedTableName)

  private implicit val getRowResult: GetResult[RawRow] = GetResult { r =>
    val objectsAndStrings = tableSpec.columnNames.map { col =>
      val rawObj = r.nextObject()
      // PgArray holds a reference to the source JDBC connection. After streaming ends, that
      // connection may be closed, causing "This connection has been closed" in flush().
      // Convert to PGobject (no connection reference) while the connection is still alive.
      val obj    = rawObj match {
        case arr: java.sql.Array =>
          val pgObj = new PGobject()
          pgObj.setType(arr.getBaseTypeName + "[]")
          pgObj.setValue(rawObj.toString)
          pgObj
        case other               => other
      }
      val str    = if (rawObj == null) null else rawObj.toString
      (col, obj, str)
    }
    RawRow(
      objects = objectsAndStrings.map { case (col, obj, _) => col -> obj }.toMap,
      strings = objectsAndStrings.map { case (col, _, str) => col -> str }.toMap
    )
  }

  override def run(context: JdbcBackend#JdbcActionContext): Int = {
    val columnList = tableSpec.columnNames.map(quoteIdentifier).mkString(", ")
    val limit      = tableSpec.limit
    val batchSize  = tableSpec.batchSize

    val selectSql = {
      val orderBy =
        limit.filter(_ => tableSpec.columnNames.contains("id")).map(_ => s" ORDER BY ${quoteIdentifier("id")} DESC").getOrElse("")
      s"SELECT $columnList FROM $quotedTableName" +
        tableSpec.whereClause.fold("")(w => s" WHERE ${w.sql}") +
        orderBy +
        limit.fold("")(n => s" LIMIT $n")
    }

    println(s"[TableCopier] SELECT: $selectSql")

    val totalRows = Await.result(dbContext.db.run(sql"SELECT count(*) FROM (#$selectSql) t".as[Int].head), Duration.Inf)
    println(s"[TableCopier] Copying table: $tableName ($totalRows rows)")

    val insertSql = {
      val columnList                                                              = columns.map(c => quoteIdentifier(c._1.name)).mkString(", ")
      val columnNames                                                             = columns.map(_._1.name)
      def onConflictActionClause(action: OnConflict.Action, strings: Seq[String]) =
        action match {
          case OnConflict.Action.DoNothing            => "DO NOTHING"
          case OnConflict.Action.DoUpdate(updateCols) =>
            "DO UPDATE SET " +
              updateCols
                .getOrElse(columnNames.toSet -- strings)
                .map(c => s"${quoteIdentifier(c)} = EXCLUDED.${quoteIdentifier(c)}")
                .mkString(", ")
        }
      def onConflictClause(onConflict: OnConflict)                                =
        for {
          cols          <- onConflict.target match {
                             case OnConflict.ConflictTarget.Constraint(name) => Future.successful(Left(name))
                             case OnConflict.ConflictTarget.Columns(cols)    => Future.successful(Right(cols))
                             case OnConflict.ConflictTarget.PrimaryKey       =>
                               dbContext.allPrimaryKeys.map(_.getOrElse(tableName, Set.empty).toSeq.sorted).map(Right(_))
                           }
          conflictTarget = cols match {
                             case Left(constraint) => s"ON CONSTRAINT ${quoteIdentifier(constraint)}"
                             case Right(cols)      => s"(${cols.map(quoteIdentifier).mkString(", ")})"
                           }
        } yield s" ON CONFLICT $conflictTarget ${onConflictActionClause(onConflict.action, cols.getOrElse(Seq.empty))}"
      for {
        onConflictStr <- tableSpec.onConflict match {
                           case Some(value) => onConflictClause(value)
                           case None        => Future.successful("")
                         }
        placeholders   = columns.map(_ => "?").mkString(", ")
      } yield s"INSERT INTO $quotedTableName ($columnList) VALUES ($placeholders)$onConflictStr"
    }

    Await.result(
      for {
        sql     <- insertSql
        inserter = new CopyAction.BatchInserter(tableName, columns, sql, batchSize, totalRows, context.connection)
        _       <-
          dbContext.db
            .stream(
              sql"#$selectSql"
                .as[RawRow]
                .transactionally
                .withStatementParameters(fetchSize = batchSize)
            )
            .foreach(inserter.receiveBatch)
      } yield inserter.flush(),
      Duration.Inf
    )
  }
}
object CopyAction {

  /** Buffers rows and inserts them in batches via JDBC prepared statements.
    *
    * Each column is paired with its database type so that JSON/JSONB values can be wrapped in [[PGobject]] before binding. Per-column writer functions are
    * precomputed at construction time to avoid per-row overhead.
    *
    * @param tableName
    *   Unquoted table name — quoted internally for SQL, used as-is for error messages.
    * @param columns
    *   Ordered sequence of (output column, database type) pairs — determines the per-column transform/wrap logic.
    * @param insertSql
    *   Fully-formed INSERT SQL statement (including any ON CONFLICT clause).
    * @param batchSize
    *   Number of rows to accumulate before flushing a batch INSERT.
    * @param conn
    *   JDBC connection on which prepared statements are created. Must remain open for the lifetime of this instance.
    */
  class BatchInserter(
      tableName: String,
      columns: Seq[(OutputColumn, String)],
      insertSql: String,
      batchSize: Int,
      totalRows: Int,
      conn: Connection
  ) {
    private val quotedTable = quoteIdentifier(tableName)
    private val buffer      = new ArrayBuffer[RawRow](batchSize)
    private var count       = 0
    private var lastLogTime = System.currentTimeMillis()

    /** Per-column functions that extract a value from a [[RawRow]] and wrap JSON/JSONB values in [[PGobject]]. */
    private val writers =
      columns.map { case (col, colType) =>
        col.lift { value =>
          if ((colType == "jsonb" || colType == "json") && value != null) {
            val jsonObj = new PGobject()
            jsonObj.setType(colType)
            jsonObj.setValue(value.toString)
            jsonObj
          } else
            value
        }
      }

    private def insertBatch(batch: Vector[RawRow]) = {
      val stmt = conn.prepareStatement(insertSql)
      try {
        for (rawRow <- batch) {
          for (idx <- columns.indices)
            stmt.setObject(idx + 1, writers(idx)(rawRow))
          stmt.addBatch()
        }
        stmt.executeBatch()
        batch.size
      } finally
        stmt.close()
    }

    /** Add a row to the buffer; flushes a batch INSERT when the buffer reaches `batchSize`. */
    def receiveBatch(row: RawRow): Unit = {
      buffer += row
      if (buffer.size >= batchSize) {
        count += insertBatch(buffer.toVector)
        buffer.clear()
        val now = System.currentTimeMillis()
        if (now - lastLogTime >= 5000) {
          println(s"[TableCopier] Inserted $count/$totalRows rows into $quotedTable...")
          lastLogTime = now
        }
      }
    }

    /** Insert any remaining buffered rows and return the total number of rows inserted across all batches. */
    def flush(): Int = {
      if (buffer.nonEmpty)
        count += insertBatch(buffer.toVector)
      println(s"[TableCopier] Copied $count/$totalRows rows from $quotedTable")
      count
    }
  }
}
