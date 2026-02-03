package simpleanonymizer

import scala.concurrent.{ExecutionContext, Future}

import simpleanonymizer.SlickProfile.api._
import simpleanonymizer.SlickProfile.quoteIdentifier

/** Copies a single table from source to target by streaming rows and inserting in batches.
  *
  * Self-referencing foreign keys are automatically detected and handled by temporarily deferring constraints during the copy transaction. Requires PostgreSQL
  * 9.4+.
  *
  * @param tableSpec
  *   `outputColumns` defines '''exactly''' which columns are SELECTed from the source and INSERTed into target — there is no automatic passthrough. Every
  *   column that should appear in the INSERT must be listed, including PK and FK columns if the target table requires them. Columns with database defaults
  *   (SERIAL, nullable, DEFAULT) can be omitted. If a required column is missing, the INSERT will fail with a database error at runtime.
  *
  * '''Note:''' [[DbCopier]] automatically adds passthrough columns for any database columns not in the spec (including PKs and FKs) before passing it to
  * `TableCopier`.
  * @param limit
  *   If set, only copy this many rows (ordered by `id` DESC if an `id` column exists).
  * @param batchSize
  *   Number of rows per INSERT batch (default 1000).
  */
case class TableCopier(
    sourceDb: Database,
    targetDb: Database,
    schema: String,
    tableName: String,
    tableSpec: TableSpec,
    limit: Option[Int] = None,
    batchSize: Int = 1000
)(implicit ec: ExecutionContext) {

  /** Get column types for a table using raw SQL, validating that all spec columns exist in the source. */
  private def columnTypesFut: Future[Seq[(OutputColumn, String)]] =
    sourceDb
      .run(
        sql"""
          SELECT column_name, data_type
          FROM information_schema.columns
          WHERE table_schema = $schema AND table_name = $tableName
        """.as[(String, String)]
      )
      .flatMap { colTypes =>
        val typeMap        = colTypes.toMap
        val missingColumns = tableSpec.columnNames.filterNot(typeMap.contains)
        if (missingColumns.nonEmpty)
          Future.failed(
            new IllegalArgumentException(
              s"Table '$tableName' spec references columns that do not exist in the source database: ${missingColumns.mkString(", ")}"
            )
          )
        else
          Future.successful(tableSpec.columns.map(c => c -> typeMap(c.name)))
      }

  private val selfRefConstraints = new SelfRefConstraints(targetDb, schema, tableName)

  def run: Future[Int] = {
    println(s"[TableCopier] Copying table: $tableName")

    val transformedColumns = tableSpec.columns.collect { case c if !c.isInstanceOf[OutputColumn.SourceColumn] => c.name }
    if (transformedColumns.nonEmpty)
      println(s"[TableCopier] Transforming columns of $tableName: ${transformedColumns.mkString(", ")}")

    selfRefConstraints.restoringDeferrability { constraints =>
      for {
        _           <- selfRefConstraints.deferAll(constraints.flatMap(_.fkName).distinct)
        columnTypes <- columnTypesFut
        copyAction   = new CopyAction(
                         sourceDb = sourceDb,
                         quotedTable = quoteIdentifier(tableName),
                         tableSpec = tableSpec,
                         columns = columnTypes,
                         limit = limit,
                         batchSize = batchSize
                       )
        count       <- targetDb.run(copyAction.transactionally)
      } yield count
    }
  }
}
