package simpleanonymizer

import scala.concurrent.{ExecutionContext, Future}

import org.slf4j.LoggerFactory
import simpleanonymizer.SlickProfile.api._
import simpleanonymizer.SlickProfile.{quoteIdentifier, quoteQualified}

/** Copies a single table from source to target by streaming rows and inserting in batches.
  *
  * Self-referencing foreign keys are automatically detected and handled by temporarily deferring constraints during the copy transaction. Requires PostgreSQL
  * 9.4+.
  *
  * @param source
  *   Schema metadata for the source database. Used for column type lookups and resolving PK-based conflict targets (`upsertOnPk` / `skipConflictOnPk`).
  */
class TableCopier(source: DbContext, val target: DbContext)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)

  private def resetSequences(tableName: String) =
    target.allSequences.flatMap { allSeqs =>
      Future
        .traverse(allSeqs.filter(_.tableName == tableName)) { seq =>
          val quotedSequence = quoteQualified(target.schema, seq.sequenceName)
          val quotedColumn   = quoteIdentifier(seq.columnName)
          val quotedTable    = quoteQualified(target.schema, tableName)
          target.db
            .run(
              sql"SELECT setval('#$quotedSequence', coalesce(max(#$quotedColumn), 0) + 1, false) FROM #$quotedTable"
                .as[Long]
                .head
            )
            .map(newVal => logger.info("Reset sequence {} to {} for {}.{}", seq.sequenceName, newVal, tableName, seq.columnName))
        }
    }

  /** @param tableSpec
    *   `columns` define <b>exactly</b> which columns are SELECTed from the source and INSERTed into target — there is no automatic passthrough. Every column
    *   that should appear in the INSERT must be listed, including PK and FK columns if the target table requires them. Columns with database defaults (SERIAL,
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

    val transformedColumns = tableSpec.columns.collect { case c if !c.isInstanceOf[OutputColumn.SourceColumn] => c.name }
    if (transformedColumns.nonEmpty)
      logger.info("Transforming columns of {}: {}", tableName, transformedColumns.mkString(", "))

    target.allForeignKeys.flatMap { allFks =>
      val selfRefFks = allFks.filter(fk => fk.pkTable == fk.fkTable && fk.fkTable.name == tableName)
      new ConstraintDeferrer(target.db).withDeferredConstraints(selfRefFks) {
        for {
          columnTypes <- columnTypesFut
          copyAction   = new CopyAction(
                           sourceDbContext = source,
                           targetSchema = target.schema,
                           tableName = tableName,
                           tableSpec = tableSpec,
                           columns = columnTypes
                         )
          count       <- target.db.run(copyAction.transactionally)
          _           <- resetSequences(tableName)
        } yield count
      }
    }
  }
}
