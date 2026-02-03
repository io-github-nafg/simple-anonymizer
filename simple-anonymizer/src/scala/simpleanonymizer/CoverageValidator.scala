package simpleanonymizer

import slick.dbio.DBIO
import slick.jdbc.meta.{MColumn, MForeignKey, MPrimaryKey, MQName}

import scala.concurrent.ExecutionContext

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

  /** List columns that need explicit handling in a [[TableSpec]] when used with [[DbCopier]].
    *
    * PK and FK columns are excluded because [[DbCopier]] passes them through automatically.
    */
  def getDataColumns(table: MQName)(implicit ec: ExecutionContext): DBIO[Seq[String]] =
    for {
      columns   <- MColumn.getColumns(table, "%")
      pkColumns <- MPrimaryKey.getPrimaryKeys(table)
      fkColumns <- MForeignKey.getImportedKeys(table)
    } yield {
      val pkColumnNames = pkColumns.map(_.column).toSet
      val fkColumnNames = fkColumns.map(_.fkColumn).toSet
      columns.map(_.name).filterNot(c => pkColumnNames.contains(c) || fkColumnNames.contains(c))
    }

  def ensureAllColumns(tableSpecs: Map[String, TableSpec], mtableMap: Map[String, MQName])(implicit ec: ExecutionContext): DBIO[Unit] =
    DBIO
      .traverse(tableSpecs.toSeq) { case (tableName, spec) =>
        getDataColumns(mtableMap(tableName))
          .map(cols => tableName -> spec.validateCovers(cols.toSet))
      }
      .flatMap { results =>
        val failures = results.collect { case (tableName, Left(missing)) => (tableName, missing) }

        if (failures.isEmpty)
          DBIO.unit
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

          DBIO.failed(new IllegalArgumentException(errorMsg))
        }
      }

  def ensureAllTables(tables: Seq[MQName], skippedTables: Set[String], copiedTables: Set[String])(implicit ec: ExecutionContext): DBIO[Unit] = {
    val requiredTables = tables.filterNot(t => skippedTables.contains(t.name))
    val missingTables  = requiredTables.filterNot(t => copiedTables.contains(t.name))

    if (missingTables.isEmpty)
      DBIO.unit
    else
      DBIO
        .traverse(missingTables) { table =>
          getDataColumns(table).map(table -> _)
        }
        .flatMap { tableColumns =>
          val snippets = tableColumns.map { case (table, cols) => generateTableSnippet(table.name, cols) }
          val skipList = missingTables.map(t => s""""${t.name}"""").mkString(", ")
          val errorMsg =
            s"""Missing table specs for ${missingTables.size} table(s).
               |
               |Add these tables to copier.run(...):
               |
               |${snippets.mkString(",\n\n")}
               |
               |Or skip them via DbCopier(skippedTables = Set($skipList))
               |""".stripMargin

          DBIO.failed(new IllegalArgumentException(errorMsg))
        }
  }
}
