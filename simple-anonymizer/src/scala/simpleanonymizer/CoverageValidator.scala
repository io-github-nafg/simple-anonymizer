package simpleanonymizer

import slick.jdbc.meta.MForeignKey

import scala.concurrent.{ExecutionContext, Future}

class CoverageValidator private (dbContext: DbContext)(implicit ec: ExecutionContext) {
  import CoverageValidator._

  private lazy val fkColumnsByTableFut: Future[Map[String, Set[String]]] =
    dbContext.allForeignKeys.map(fkColumnsByTable)

  /** List columns that need explicit handling in a [[TableSpec]] when used with [[DbCopier]].
    *
    * PK and FK columns are excluded because [[DbCopier]] passes them through automatically.
    */
  def getDataColumns(tableName: String): Future[Seq[String]] =
    for {
      allColumns    <- dbContext.allColumns
      allPKs        <- dbContext.allPrimaryKeys
      fkColsByTable <- fkColumnsByTableFut
    } yield {
      val columns       = allColumns.getOrElse(tableName, Seq.empty)
      val pkColumnNames = allPKs.getOrElse(tableName, Set.empty)
      val fkColumnNames = fkColsByTable.getOrElse(tableName, Set.empty)
      columns.filterNot(c => pkColumnNames.contains(c) || fkColumnNames.contains(c))
    }

  private def ensureAllColumns(tableSpecs: Map[String, TableSpec]): Future[Unit] =
    dbContext.allColumns.flatMap { allColumns =>
      val results  = tableSpecs.toSeq.map { case (tableName, spec) =>
        val cols = allColumns.getOrElse(tableName, Seq.empty)
        tableName -> spec.validateCovers(cols.toSet)
      }
      val failures = results.collect { case (tableName, Left(missing)) => (tableName, missing) }
      if (failures.isEmpty)
        Future.unit
      else {
        val failureMessages = failures.map { case (tableName, missing) =>
          s"""Table '$tableName' is missing ${missing.size} column(s). Add these:
             |      ${generateColumnSnippets(missing)}""".stripMargin
        }
        val errorMsg        =
          s"""Table specs are missing columns for ${failures.size} table(s).
             |
             |${failureMessages.mkString("\n\n")}
             |""".stripMargin
        Future.failed(new IllegalArgumentException(errorMsg))
      }
    }

  private def ensureAllTables(
      tableNames: Seq[String],
      skippedTables: Set[String],
      copiedTables: Set[String]
  ): Future[Unit] = {
    val requiredTables = tableNames.filterNot(skippedTables.contains)
    val missingTables  = requiredTables.filterNot(copiedTables.contains)

    if (missingTables.isEmpty)
      Future.unit
    else
      Future
        .traverse(missingTables)(t => getDataColumns(t).map(cols => generateTableSnippet(t, cols)))
        .flatMap { snippets =>
          val skipList = missingTables.map(t => s""""$t"""").mkString(", ")
          val errorMsg =
            s"""Missing table specs for ${missingTables.size} table(s).
               |
               |Add these tables to copier.run(...):
               |
               |${snippets.mkString(",\n\n")}
               |
               |Or skip them via DbCopier(skippedTables = Set($skipList))
               |""".stripMargin
          Future.failed(new IllegalArgumentException(errorMsg))
        }
  }

  /** Validate that all tables have specs and all specs cover their required columns. */
  def validate(
      tableNames: Seq[String],
      skippedTables: Set[String],
      tableSpecs: Map[String, TableSpec]
  ): Future[Unit] =
    for {
      _ <- ensureAllTables(tableNames, skippedTables, tableSpecs.keySet)
      _ <- ensureAllColumns(tableSpecs)
      _  = println(s"[CoverageValidator] Validation passed.")
    } yield ()
}

object CoverageValidator {

  /** Generate a code snippet for a table transformer with passthrough for all columns. */
  def generateTableSnippet(tableName: String, columns: Seq[String]): String = {
    val columnList =
      if (columns.isEmpty) ""
      else columns.map(col => s"      row.$col").mkString(",\n")
    s""""$tableName" -> TableSpec.select { row =>\n    Seq(\n$columnList\n    )\n  }"""
  }

  /** Generate code snippets for missing column bindings. */
  def generateColumnSnippets(columns: Set[String]): String =
    columns.toSeq.sorted.map(col => s"row.$col").mkString(",\n      ")

  private[simpleanonymizer] def fkColumnsByTable(allFks: Seq[MForeignKey]): Map[String, Set[String]] =
    allFks.groupBy(_.fkTable.name).map { case (table, fks) => table -> fks.map(_.fkColumn).toSet }

  def apply(dbContext: DbContext)(implicit ec: ExecutionContext): CoverageValidator =
    new CoverageValidator(dbContext)
}
