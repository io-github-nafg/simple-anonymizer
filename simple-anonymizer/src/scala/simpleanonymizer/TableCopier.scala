package simpleanonymizer

import scala.concurrent.{ExecutionContext, Future}

import simpleanonymizer.SlickProfile.api._

/** Copies a single table from source to target by streaming rows and inserting in batches.
  *
  * Self-referencing foreign keys are automatically detected and handled by temporarily deferring constraints during the copy transaction. Requires PostgreSQL
  * 9.4+.
  *
  * @param source
  *   Schema metadata for the source database. Used for column type lookups and resolving PK-based conflict targets (`upsertOnPk` / `skipConflictOnPk`).
  */
class TableCopier(source: DbContext, val targetDb: Database)(implicit ec: ExecutionContext) {
  /*
   @param tableSpec
   *   `columns` define <b>exactly</b> which columns are SELECTed from the source and INSERTed into target — there is no automatic passthrough. Every column that
   *   should appear in the INSERT must be listed, including PK and FK columns if the target table requires them. Columns with database defaults (SERIAL,
   *   nullable, DEFAULT) can be omitted. If a required column is missing, the INSERT will fail with a database error at runtime.
   *
   * <b>Note:</b> [[DbCopier]] automatically adds passthrough columns for any database columns not in the spec (including PKs and FKs).
   */
  def run(tableName: String, tableSpec: TableSpec): Future[Int] = {

    def columnTypesFut: Future[Seq[(OutputColumn, String)]] =
      source.columnTypesFor(tableName).flatMap { typeMap =>
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

    val selfRefConstraints = new SelfRefConstraints(targetDb, source.schema, tableName)

    val transformedColumns = tableSpec.columns.collect { case c if !c.isInstanceOf[OutputColumn.SourceColumn] => c.name }
    if (transformedColumns.nonEmpty)
      println(s"[TableCopier] Transforming columns of $tableName: ${transformedColumns.mkString(", ")}")

    selfRefConstraints.restoringDeferrability { constraints =>
      for {
        _           <- selfRefConstraints.deferAll(constraints.flatMap(_.fkName).distinct)
        columnTypes <- columnTypesFut
        copyAction   = new CopyAction(
                         dbContext = source,
                         tableName = tableName,
                         tableSpec = tableSpec,
                         columns = columnTypes
                       )
        count       <- targetDb.run(copyAction.transactionally)
      } yield count
    }
  }
}
