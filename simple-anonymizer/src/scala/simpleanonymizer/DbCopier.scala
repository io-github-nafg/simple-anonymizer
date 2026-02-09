package simpleanonymizer

import simpleanonymizer.SlickProfile.api._
import slick.jdbc.meta.MTable

import scala.concurrent.{ExecutionContext, Future}

/** High-level orchestrator for copying a database snapshot with subsetting and anonymization.
  *
  * ==Column handling==
  *
  * Each `TableSpec` only needs to specify '''non-PK, non-FK columns'''. Primary key and foreign key columns are automatically passed through (copied as-is). If
  * you do include a PK or FK column in your spec, it takes precedence over the automatic passthrough — this lets you transform PK/FK values if needed (e.g.,
  * for subsetting or anonymization of self-referencing columns).
  *
  * Validation (via [[CoverageValidator]]) enforces that every non-PK, non-FK column is covered by the spec. Missing columns produce an error with copy-pastable
  * code snippets.
  *
  * '''Note:''' This is different from using [[TableCopier]] directly, which requires the `TableSpec` to list ''all'' columns that will appear in the INSERT
  * statement (including PKs and FKs), since it has no automatic passthrough behavior.
  *
  * @example
  *   {{{
  * import simpleanonymizer.Anonymizer
  *
  * val copier = new DbCopier(sourceDb, targetDb)
  *
  * // Only non-PK/non-FK columns need to be listed.
  * // PK (id) and FK (user_id in orders) are copied automatically.
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

  private val sourceDbContext = new DbContext(sourceDb, schema)

  private def copyTablesInOrder(
      tables: Seq[MTable],
      skippedTables: Set[String],
      specs: Map[String, TableSpec],
      filters: Map[String, Option[String]]
  ): Future[Map[String, Int]] = {
    println(s"[DbCopier] Copying ${tables.size} tables...")

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
              allColumnNames <- sourceDbContext.db.run(table.getColumns.map(_.map(_.name)))
              tableSpec       = specs.getOrElse(tableName, TableSpec(Seq.empty))
              // Merge user-specified output columns with passthrough for any remaining columns
              fullTableSpec   = TableSpec(
                                  columns = {
                                    val userOutputColumns = tableSpec.columns
                                    val specifiedNames    = userOutputColumns.map(_.name)
                                    val passthroughCols   = allColumnNames.diff(specifiedNames).map(OutputColumn.SourceColumn(_))
                                    passthroughCols ++ userOutputColumns
                                  },
                                  whereClause = filters.getOrElse(tableName, None),
                                  limit = tableSpec.limit,
                                  batchSize = tableSpec.batchSize
                                )
              tableCopier     = new TableCopier(
                                  source = sourceDbContext,
                                  targetDb = targetDb,
                                  tableName = tableName,
                                  tableSpec = fullTableSpec
                                )
              count          <- tableCopier.run
              result         <- copyNext(rest, acc + (tableName -> count))
            } yield result
      }

    copyNext(tables.toList, Map.empty)
  }

  /** Copy all tables from source to target with transformation (preferred API).
    *
    * Tables are copied in topological order based on FK dependencies. Filters are automatically propagated from parent tables to child tables through FK
    * relationships.
    *
    * For each table, the provided `TableSpec` columns are merged with automatic passthrough columns for any remaining database columns not in the spec. This
    * means PK and FK columns do not need to be listed — they are included automatically. If a PK or FK column ''is'' listed in the spec, it overrides the
    * automatic passthrough for that column.
    *
    * @param tableSpecs
    *   Each entry is a table name paired with a TableSpec created via `TableSpec.select { row => ... }`. Only non-PK/non-FK columns need to be specified; PK
    *   and FK columns are passed through automatically. Use `skippedTables` constructor parameter to skip tables entirely.
    * @return
    *   A successful Future containing a map of table names to the number of rows copied. Or, if any table in the database is not specified, or if a spec is
    *   missing non-PK/non-FK columns, the Future fails with IllegalArgumentException. Error messages include copy-pastable code snippets.
    */
  def run(tableSpecs: (String, TableSpec)*): Future[Map[String, Int]] = {
    val tableSpecsMap = tableSpecs.toMap

    val validator = CoverageValidator(sourceDbContext)

    for {
      tables       <- sourceDbContext.allTables
      fks          <- sourceDbContext.allForeignKeys
      orderedTables = TableSorter(tables, fks).flatten
      filters       = FilterPropagation.computeEffectiveFilters(orderedTables.map(_.name.name), fks, tableSpecsMap)
      _            <- validator.validate(tables.map(_.name.name), skippedTables, tableSpecsMap)
      result       <- copyTablesInOrder(orderedTables, skippedTables, tableSpecsMap, filters)
    } yield result
  }
}
