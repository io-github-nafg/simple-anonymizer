package simpleanonymizer

import simpleanonymizer.SlickProfile.api._
import slick.jdbc.meta.MTable

import scala.concurrent.{ExecutionContext, Future}

/** High-level orchestrator for copying a database snapshot with subsetting and anonymization.
  *
  * Requires explicit handling for all tables - either a transformer or explicit skip. This ensures every column is consciously handled.
  *
  * @example
  *   {{{
  * import simpleanonymizer.Anonymizer
  *
  * val copier = new DbCopier(sourceDb, targetDb)
  *
  * for {
  *   result <- copier.run(
  *     "users"  -> TableSpec.select { row =>
  *       Seq(
  *         row.first_name.mapString(Anonymizer.FirstName),
  *         row.last_name.mapString(Anonymizer.LastName),
  *         row.email.mapString(Anonymizer.Email)
  *       )
  *     }.where("id <= 10"),
  *     "orders" -> TableSpec.select { row =>
  *       Seq(row.description, row.status)
  *     }
  *   )
  * } yield result  // Map[tableName -> rowCount]
  *   }}}
  */
class DbCopier(sourceDb: Database, targetDb: Database, schema: String = "public", skippedTables: Set[String] = Set.empty)(implicit ec: ExecutionContext) {

  private val dbMetadata = new DbMetadata(schema)

  private def copyTablesInOrder(
      tables: Seq[MTable],
      skippedTables: Set[String],
      specs: Map[String, TableSpec],
      filters: Map[String, Option[String]]
  ): Future[Map[String, Int]] = {

    def copyNext(remaining: List[MTable], acc: Map[String, Int]): Future[Map[String, Int]] =
      remaining match {
        case Nil           => Future.successful(acc)
        case table :: rest =>
          val tableName = table.name.name
          if (skippedTables.contains(tableName)) {
            println(s"[DbCopier] Skipping table: $tableName")
            copyNext(rest, acc + (tableName -> 0))
          } else
            for {
              columns <- sourceDb.run(table.getColumns.map(_.map(_.name)))
              count   <- TableCopier.copyTable(
                           sourceDb = sourceDb,
                           targetDb = targetDb,
                           schema = schema,
                           tableName = tableName,
                           columns = columns,
                           whereClause = filters.getOrElse(tableName, None),
                           tableSpec = specs.get(tableName)
                         )
              result  <- copyNext(rest, acc + (tableName -> count))
            } yield result
      }

    copyNext(tables.toList, Map.empty)
  }

  /** Copy all tables from source to target with transformation (preferred API).
    *
    * Tables are copied in topological order based on FK dependencies. Filters are automatically propagated from parent tables to child tables through FK
    * relationships.
    *
    * @param tableSpecs
    *   Each entry is a table name paired with a TableSpec created via `TableSpec.select { row => ... }`. Use `skippedTables` constructor parameter to skip
    *   tables.
    * @return
    *   A successful Future containing a map of table names to the number of rows copied. Or, if any table in the database is not specified, or if a transformer
    *   is missing columns, the Future fails with IllegalArgumentException. Error messages include copy-pastable code snippets.
    */
  def run(tableSpecs: (String, TableSpec)*): Future[Map[String, Int]] = {
    val tableSpecsMap = tableSpecs.toMap

    for {
      tables       <- sourceDb.run(dbMetadata.getAllTables)
      fks          <- sourceDb.run(dbMetadata.getAllForeignKeys)
      orderedTables = TableSorter(tables, fks).flatten
      filters       = FilterPropagation.computeEffectiveFilters(orderedTables.map(_.name.name), fks, tableSpecsMap)
      // Validate that all non-skipped tables have transformers
      _            <- sourceDb.run(CoverageValidator.ensureAllTables(tables.map(_.name), skippedTables, tableSpecsMap.keySet))
      // Validate that each transformer covers all required columns
      _            <- sourceDb.run(CoverageValidator.ensureAllColumns(tableSpecsMap, tables.map(t => t.name.name -> t.name).toMap))
      // Copy tables level by level
      result       <- copyTablesInOrder(orderedTables, skippedTables, tableSpecsMap, filters)
    } yield result
  }
}
