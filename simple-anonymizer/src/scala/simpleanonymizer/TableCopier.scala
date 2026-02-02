package simpleanonymizer

import org.postgresql.util.PGobject
import simpleanonymizer.SlickProfile.api._
import simpleanonymizer.SlickProfile.quoteIdentifier
import slick.jdbc.GetResult

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object TableCopier {

  /** Wrap a value as a PGobject if the column type is JSON/JSONB, otherwise return as-is */
  private def wrapIfJson(value: AnyRef, columnType: String): AnyRef =
    if (value != null && (columnType == "jsonb" || columnType == "json")) {
      val jsonObj = new PGobject()
      jsonObj.setType(columnType)
      jsonObj.setValue(value.toString)
      jsonObj
    } else
      value

  private val batchSize = 1000

  private abstract class BatchInserter(tableName: String) {
    val buffer = new ArrayBuffer[RawRow](batchSize)
    var count  = 0

    def insertBatch(batch: Vector[RawRow]): Int

    def receiveBatch(row: RawRow): Unit = {
      buffer += row
      if (buffer.size >= batchSize) {
        count += insertBatch(buffer.toVector)
        buffer.clear()
        if (count % 1000 == 0)
          println(s"[TableCopier] Inserted $count rows into $tableName...")
      }
    }

    def flush() = {
      if (buffer.nonEmpty)
        count += insertBatch(buffer.toVector)
      println(s"[TableCopier] Copied $count rows from $tableName")
      count
    }
  }

  def copyTable(
      sourceDb: Database,
      targetDb: Database,
      schema: String,
      tableName: String,
      columns: Seq[String],
      whereClause: Option[String] = None,
      limit: Option[Int] = None,
      tableSpec: Option[TableSpec] = None
  )(implicit ec: ExecutionContext): Future[Int] = {

    /** Get column types for a table using raw SQL */
    val columnTypesAction: DBIO[Seq[String]] =
      sql"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $schema AND table_name = $tableName
      """.as[(String, String)].map { colTypes =>
        val typeMap = colTypes.toMap
        columns.map(c => typeMap.getOrElse(c, "unknown"))
      }

    implicit val getRowResult: GetResult[RawRow] = GetResult { r =>
      val objectsAndStrings = columns.map { col =>
        val obj = r.nextObject()
        val str = if (obj == null) null else obj.toString
        (col, obj, str)
      }
      RawRow(
        objects = objectsAndStrings.map { case (col, obj, _) => col -> obj }.toMap,
        strings = objectsAndStrings.map { case (col, _, str) => col -> str }.toMap
      )
    }

    val quotedTable  = quoteIdentifier(tableName)
    val columnList   = columns.map(quoteIdentifier).mkString(", ")
    val placeholders = columns.map(_ => "?").mkString(", ")

    val orderBy =
      limit.filter(_ => columns.contains("id")).map(_ => s" ORDER BY ${quoteIdentifier("id")} DESC").getOrElse("")

    val selectSql =
      s"SELECT $columnList FROM $quotedTable" +
        whereClause.fold("")(w => s" WHERE $w") +
        orderBy +
        limit.fold("")(n => s" LIMIT $n")
    val insertSql = s"INSERT INTO $quotedTable ($columnList) VALUES ($placeholders)"

    println(s"[TableCopier] Copying table: $tableName")
    println(s"[TableCopier] SELECT: $selectSql")
    tableSpec.foreach(t => println(s"[TableCopier] Transforming columns: ${t.outputColumns.mkString(", ")}"))

    def buildWriters(columnTypes: Seq[String]): Vector[RawRow => AnyRef] = {
      // Build a map of column name -> OutputColumn for a quick lookup
      val columnPlans: Map[String, OutputColumn] =
        tableSpec.map(_.columns.map(plan => plan.name -> plan).toMap).getOrElse(Map.empty)

      // Precompute per-column writers to avoid pattern matching in the hot loop
      columns.indices.map { idx =>
        val column     = columns(idx)
        val columnType = columnTypes(idx)

        columnPlans.get(column) match {
          case Some(plan) => plan.transform(wrapIfJson(_, columnType))
          case None       =>
            // Column not in transformer - use the original raw object
            (rawRow: RawRow) => wrapIfJson(rawRow.objects.getOrElse(column, null), columnType)
        }
      }.toVector
    }

    def batchInsertAction(batch: Vector[RawRow], writers: Vector[RawRow => AnyRef]) =
      SimpleDBIO[Int] { ctx =>
        val stmt = ctx.connection.prepareStatement(insertSql)
        for (rawRow <- batch) {
          for (idx <- columns.indices)
            stmt.setObject(idx + 1, writers(idx)(rawRow))
          stmt.addBatch()
        }
        stmt.executeBatch()
        stmt.close()
        batch.size
      }

    for {
      columnTypes  <- sourceDb.run(columnTypesAction)
      batchInserter = new BatchInserter(tableName) {
                        val writers = buildWriters(columnTypes)

                        override def insertBatch(batch: Vector[RawRow]) = {
                          val action = batchInsertAction(batch, writers)
                          Await.result(targetDb.run(action), Duration.Inf)
                        }
                      }
      action        = sql"#$selectSql".as[RawRow].transactionally.withStatementParameters(fetchSize = batchSize)
      _            <- sourceDb.stream(action).foreach(batchInserter.receiveBatch)
    } yield batchInserter.flush()
  }
}
