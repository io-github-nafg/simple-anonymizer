package simpleanonymizer

import java.sql.DatabaseMetaData

import scala.concurrent.{ExecutionContext, Future}

import slick.jdbc.meta.{MForeignKey, MQName}

import simpleanonymizer.SlickProfile.api._
import simpleanonymizer.SlickProfile.quoteIdentifier

/** Handles saving, deferring, and restoring self-referencing FK constraints on a single table. */
private[simpleanonymizer] class SelfRefConstraints(db: Database, schema: String, tableName: String)(implicit
    ec: ExecutionContext
) {
  private val quotedTable = CopyAction.qualifiedTable(schema, tableName)

  /** ALTER constraints to DEFERRABLE INITIALLY DEFERRED (requires PostgreSQL 9.4+) */
  def deferAll(constraintNames: Seq[String]): Future[Seq[Int]] =
    Future
      .traverse(constraintNames) { name =>
        db.run(sqlu"ALTER TABLE #$quotedTable ALTER CONSTRAINT #${quoteIdentifier(name)} DEFERRABLE INITIALLY DEFERRED")
      }
      .recoverWith { case e =>
        Future.failed(
          new RuntimeException(
            s"Failed to make constraints deferrable on $tableName. ALTER TABLE ... ALTER CONSTRAINT requires PostgreSQL 9.4+.",
            e
          )
        )
      }

  /** Query JDBC metadata for foreign keys where the table references itself. */
  private def selfRefKeys: DBIO[Seq[MForeignKey]] =
    MForeignKey
      .getImportedKeys(MQName(None, Some(schema), tableName))
      .map { allFks =>
        val selfRefFks = allFks.filter(fk => fk.pkTable.name == fk.fkTable.name)

        if (selfRefFks.nonEmpty)
          println(s"[TableCopier] Self-ref constraints for $tableName: ${selfRefFks.flatMap(_.fkName).mkString(", ")}")

        selfRefFks
      }

  /** ALTER each constraint back to its original deferrability state. Failures are logged but don't propagate. */
  private def restoreDeferrability(constraints: Seq[MForeignKey]): Future[Seq[Int]] = {
    val namedConstraints = constraints.flatMap(fk => fk.fkName.map(_ -> fk.deferrability)).distinct
    Future.traverse(namedConstraints) { case (name, deferrability) =>
      val quotedName = quoteIdentifier(name)
      val alterSql   = deferrability match {
        case DatabaseMetaData.importedKeyInitiallyImmediate => sqlu"ALTER TABLE #$quotedTable ALTER CONSTRAINT #$quotedName DEFERRABLE INITIALLY IMMEDIATE"
        case DatabaseMetaData.importedKeyInitiallyDeferred  => sqlu"ALTER TABLE #$quotedTable ALTER CONSTRAINT #$quotedName DEFERRABLE INITIALLY DEFERRED"
        case _                                              => sqlu"ALTER TABLE #$quotedTable ALTER CONSTRAINT #$quotedName NOT DEFERRABLE"
      }
      db.run(alterSql)
        .recover { case e =>
          println(s"[TableCopier] Warning: failed to restore constraint $name: ${e.getMessage}")
          0
        }
    }
  }

  /** Acquire a resource, run the body, then release — regardless of success or failure. */
  private def bracketFuture[R, A](acquire: => Future[R])(release: R => Future[Any])(body: R => Future[A]): Future[A] =
    acquire.flatMap { t =>
      body(t)
        .transformWith { outcome =>
          release(t)
            .transform(_ => outcome)
        }
    }

  /** Save self-ref constraint states, run body, restore original states.
    *
    * Queries imported foreign keys for this table, filters for self-referencing ones, and passes them to the body. After the body completes (success or
    * failure), each constraint is restored to its original deferrability state.
    */
  def restoringDeferrability[T](body: Seq[MForeignKey] => Future[T]): Future[T] =
    bracketFuture(db.run(selfRefKeys))(restoreDeferrability)(body)
}
