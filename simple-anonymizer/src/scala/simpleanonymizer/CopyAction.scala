package simpleanonymizer

import java.sql.Connection

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

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
    sourceDb: Database,
    quotedTable: String,
    tableSpec: TableSpec,
    columns: Seq[(OutputColumn, String)],
    limit: Option[Int],
    batchSize: Int
)(implicit executionContext: ExecutionContext)
    extends SynchronousDatabaseAction[Int, NoStream, JdbcBackend#JdbcActionContext, JdbcBackend#JdbcStreamingActionContext, Effect.All] {

  override type StreamState = Null

  override def getDumpInfo = DumpInfo(name = "CopyAction", mainInfo = quotedTable)

  private implicit val getRowResult: GetResult[RawRow] = GetResult { r =>
    val objectsAndStrings = tableSpec.columnNames.map { col =>
      val obj = r.nextObject()
      val str = if (obj == null) null else obj.toString
      (col, obj, str)
    }
    RawRow(
      objects = objectsAndStrings.map { case (col, obj, _) => col -> obj }.toMap,
      strings = objectsAndStrings.map { case (col, _, str) => col -> str }.toMap
    )
  }

  override def run(context: JdbcBackend#JdbcActionContext): Int = {
    val columnList = tableSpec.columnNames.map(quoteIdentifier).mkString(", ")

    val selectSql = {
      val orderBy =
        limit.filter(_ => tableSpec.columnNames.contains("id")).map(_ => s" ORDER BY ${quoteIdentifier("id")} DESC").getOrElse("")
      s"SELECT $columnList FROM $quotedTable" +
        tableSpec.whereClause.fold("")(w => s" WHERE $w") +
        orderBy +
        limit.fold("")(n => s" LIMIT $n")
    }

    println(s"[TableCopier] SELECT: $selectSql")

    val totalRows = Await.result(sourceDb.run(sql"SELECT count(*) FROM (#$selectSql) t".as[Int].head), Duration.Inf)
    println(s"[TableCopier] Copying table: $quotedTable ($totalRows rows)")

    val inserter = new CopyAction.BatchInserter(quotedTable, columns, batchSize, totalRows, context.connection)

    Await.result(
      sourceDb
        .stream(
          sql"#$selectSql"
            .as[RawRow]
            .transactionally
            .withStatementParameters(fetchSize = batchSize)
        )
        .foreach(inserter.receiveBatch),
      Duration.Inf
    )

    inserter.flush()
  }
}
object CopyAction {

  /** Buffers rows and inserts them in batches via JDBC prepared statements.
    *
    * Each column is paired with its database type so that JSON/JSONB values can be wrapped in [[PGobject]] before binding. Per-column writer functions are
    * precomputed at construction time to avoid per-row overhead.
    *
    * @param quotedTable
    *   Already-quoted table identifier for use in SQL statements.
    * @param columns
    *   Ordered sequence of (output column, database type) pairs — determines both the INSERT column list and the per-column transform/wrap logic.
    * @param batchSize
    *   Number of rows to accumulate before flushing a batch INSERT.
    * @param conn
    *   JDBC connection on which prepared statements are created. Must remain open for the lifetime of this instance.
    */
  class BatchInserter(
      quotedTable: String,
      columns: Seq[(OutputColumn, String)],
      batchSize: Int,
      totalRows: Int,
      conn: Connection
  ) {
    private val buffer      = new ArrayBuffer[RawRow](batchSize)
    private var count       = 0
    private var lastLogTime = System.currentTimeMillis()

    private val columnList = columns.map(c => quoteIdentifier(c._1.name)).mkString(", ")

    private val insertSql: String = {
      val placeholders = columns.map(_ => "?").mkString(", ")
      s"INSERT INTO $quotedTable ($columnList) VALUES ($placeholders)"
    }

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
