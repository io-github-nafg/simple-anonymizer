package simpleanonymizer

import slick.dbio.DBIO
import slick.jdbc.meta.MForeignKey

import scala.concurrent.ExecutionContext

class CoverageValidator private (
    allColumns: Map[String, Seq[String]],
    allPrimaryKeys: Map[String, Set[String]],
    fkColumnsByTable: Map[String, Set[String]]
) {
  import CoverageValidator._

  /** List columns that need explicit handling in a [[TableSpec]] when used with [[DbCopier]].
    *
    * PK and FK columns are excluded because [[DbCopier]] passes them through automatically.
    */
  def getDataColumns(tableName: String): Seq[String] = {
    val columns       = allColumns.getOrElse(tableName, Seq.empty)
    val pkColumnNames = allPrimaryKeys.getOrElse(tableName, Set.empty)
    val fkColumnNames = fkColumnsByTable.getOrElse(tableName, Set.empty)
    columns.filterNot(c => pkColumnNames.contains(c) || fkColumnNames.contains(c))
  }

  def ensureAllColumns(tableSpecs: Map[String, TableSpec]): Either[IllegalArgumentException, Unit] = {
    val failures = tableSpecs.toSeq
      .map { case (tableName, spec) =>
        tableName -> spec.validateCovers(getDataColumns(tableName).toSet)
      }
      .collect { case (tableName, Left(missing)) => (tableName, missing) }

    if (failures.isEmpty)
      Right(())
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

      Left(new IllegalArgumentException(errorMsg))
    }
  }

  def ensureAllTables(
      tableNames: Seq[String],
      skippedTables: Set[String],
      copiedTables: Set[String]
  ): Either[IllegalArgumentException, Unit] = {
    val requiredTables = tableNames.filterNot(skippedTables.contains)
    val missingTables  = requiredTables.filterNot(copiedTables.contains)

    if (missingTables.isEmpty)
      Right(())
    else {
      val snippets = missingTables.map(t => generateTableSnippet(t, getDataColumns(t)))
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

      Left(new IllegalArgumentException(errorMsg))
    }
  }
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

  def fkColumnsByTable(allFks: Seq[MForeignKey]): Map[String, Set[String]] =
    allFks.groupBy(_.fkTable.name).map { case (table, fks) => table -> fks.map(_.fkColumn).toSet }

  def apply(dbMetadata: DbMetadata, allFks: Seq[MForeignKey])(implicit ec: ExecutionContext): DBIO[CoverageValidator] =
    for {
      allColumns     <- dbMetadata.getAllColumns
      allPrimaryKeys <- dbMetadata.getAllPrimaryKeys
    } yield new CoverageValidator(allColumns, allPrimaryKeys, fkColumnsByTable(allFks))
}
