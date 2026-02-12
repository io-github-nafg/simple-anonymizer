package simpleanonymizer

import java.sql.DatabaseMetaData

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import slick.jdbc.meta.MForeignKey

import org.slf4j.LoggerFactory
import simpleanonymizer.SlickProfile.api._
import simpleanonymizer.SlickProfile.{quoteIdentifier, quoteQualified}

/** Utility for temporarily deferring FK constraints during operations that may violate them transiently.
  *
  * Callers provide the constraints to defer — this class handles the ALTER TABLE lifecycle. Use [[withDeferredConstraints]] for bracket-style usage that
  * automatically restores original deferrability states.
  */
class ConstraintDeferrer(db: Database)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)

  /** ALTER constraints to DEFERRABLE INITIALLY DEFERRED (requires PostgreSQL 9.4+) */
  private def deferAll(constraints: Seq[MForeignKey]) = {
    val named = constraints.flatMap(fk => fk.fkName.map(fk.fkTable -> _)).distinct
    DBIO
      .traverse(named) { case (table, name) =>
        sqlu"ALTER TABLE #${quoteQualified(table)} ALTER CONSTRAINT #${quoteIdentifier(name)} DEFERRABLE INITIALLY DEFERRED"
      }
      .transactionally
      .asTry
      .flatMap {
        case Success(value) => DBIO.successful(value)
        case Failure(e)     =>
          val tableNames = named.map(_._1.name).distinct.mkString(", ")
          DBIO.failed(
            new RuntimeException(
              s"Failed to make constraints deferrable on $tableNames. ALTER TABLE ... ALTER CONSTRAINT requires PostgreSQL 9.4+.",
              e
            )
          )
      }
  }

  /** ALTER each constraint back to its original deferrability state. Failures are logged but don't propagate. */
  private def restoreDeferrability(constraints: Seq[MForeignKey]) = {
    val namedConstraints = constraints.flatMap(fk => fk.fkName.map(name => (fk.fkTable, name, fk.deferrability))).distinct
    DBIO.traverse(namedConstraints) { case (table, name, deferrability) =>
      val qt         = quoteQualified(table)
      val quotedName = quoteIdentifier(name)
      val alterSql   = deferrability match {
        case DatabaseMetaData.importedKeyInitiallyImmediate =>
          sqlu"ALTER TABLE #$qt ALTER CONSTRAINT #$quotedName DEFERRABLE INITIALLY IMMEDIATE"
        case DatabaseMetaData.importedKeyInitiallyDeferred  =>
          sqlu"ALTER TABLE #$qt ALTER CONSTRAINT #$quotedName DEFERRABLE INITIALLY DEFERRED"
        case _                                              =>
          sqlu"ALTER TABLE #$qt ALTER CONSTRAINT #$quotedName NOT DEFERRABLE"
      }
      alterSql.asTry.map {
        case Success(value) => value
        case Failure(e)     =>
          logger.warn(s"Failed to restore constraint $name", e)
          0
      }
    }
  }

  /** Defer the given constraints, run the body, then restore original deferrability states.
    *
    * If the constraint list is empty, the body is run directly without any ALTER TABLE calls.
    *
    * @param constraints
    *   FK constraints to defer (the caller decides which ones)
    * @param body
    *   The operation to run while constraints are deferred
    */
  def withDeferredConstraints[T](constraints: Seq[MForeignKey])(body: => Future[T]): Future[T] = {
    val constraintNames = constraints.flatMap(_.fkName).distinct
    if (constraintNames.isEmpty) body
    else {
      val tableNames = constraints.map(_.fkTable.name).distinct.mkString(", ")
      logger.info("Deferring constraints for {}: {}", tableNames, constraintNames.mkString(", "))
      db.run(deferAll(constraints)).flatMap { _ =>
        Future.delegate(body).transformWith { outcome =>
          db.run(restoreDeferrability(constraints)).transform(_ => outcome)
        }
      }
    }
  }
}
