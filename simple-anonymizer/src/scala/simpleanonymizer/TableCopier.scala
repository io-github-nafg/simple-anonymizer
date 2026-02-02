package simpleanonymizer

import org.postgresql.util.PGobject
import simpleanonymizer.SlickProfile.api._
import simpleanonymizer.SlickProfile.quoteIdentifier
import slick.jdbc.GetResult

import scala.concurrent.{ExecutionContext, Future}

object TableCopier {
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
    def getColumnTypes(tableName: String)(implicit ec: ExecutionContext): DBIO[Seq[String]] =
      sql"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $schema AND table_name = $tableName
      """.as[(String, String)].map { colTypes =>
        val typeMap = colTypes.toMap
        columns.map(c => typeMap.getOrElse(c, "unknown"))
      }

    /** Wrap a value as a PGobject if the column type is JSON/JSONB, otherwise return as-is */
    def wrapIfJson(value: AnyRef, columnType: String): AnyRef =
      if (value != null && (columnType == "jsonb" || columnType == "json")) {
        val jsonObj = new PGobject()
        jsonObj.setType(columnType)
        jsonObj.setValue(value.toString)
        jsonObj
      } else
        value

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

    println(s"[TableCopier] Copying table: $tableName")
    println(s"[TableCopier] SELECT: $selectSql")
    tableSpec.foreach(t => println(s"[TableCopier] Transforming columns: ${t.outputColumns.mkString(", ")}"))

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

    val columnTypesAction = getColumnTypes(tableName)

    sourceDb.run(columnTypesAction).flatMap { columnTypes =>
      val selectAction = sql"#$selectSql".as[RawRow]

      sourceDb.run(selectAction).flatMap { rows =>
        if (rows.isEmpty) {
          println(s"[TableCopier] No rows to copy from $tableName")
          Future.successful(0)
        } else {
          val batchSize = 1000
          val batches   = rows.grouped(batchSize).toList

          // Build a map of column name -> OutputColumn for quick lookup
          val columnPlans: Map[String, OutputColumn] =
            tableSpec.map(_.columns.map(plan => plan.name -> plan).toMap).getOrElse(Map.empty)

          // Precompute per-column writers to avoid pattern matching in the hot loop
          val writers: Vector[RawRow => AnyRef] = columns.indices.map { idx =>
            val column     = columns(idx)
            val columnType = columnTypes(idx)

            columnPlans.get(column) match {
              case Some(plan) => plan.transform(wrapIfJson(_, columnType))
              case None       =>
                // Column not in transformer - use the original raw object
                (rawRow: RawRow) => wrapIfJson(rawRow.objects.getOrElse(column, null), columnType)
            }
          }.toVector

          def processBatches(remaining: List[Seq[RawRow]], count: Int): Future[Int] =
            remaining match {
              case Nil           => Future.successful(count)
              case batch :: rest =>
                val batchInsertAction = SimpleDBIO[Int] { ctx =>
                  val conn       = ctx.connection
                  val stmt       = conn.prepareStatement(insertSql)
                  var batchCount = 0

                  for (rawRow <- batch) {
                    for (idx <- columns.indices)
                      stmt.setObject(idx + 1, writers(idx)(rawRow))

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
                    println(s"[TableCopier] Inserted $newCount rows...")
                  processBatches(rest, newCount)
                }
            }

          processBatches(batches, 0).map { totalCount =>
            println(s"[TableCopier] Copied $totalCount rows from $tableName")
            totalCount
          }
        }
      }
    }
  }
}
