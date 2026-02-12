package simpleanonymizer

import org.slf4j.LoggerFactory

import slick.jdbc.meta.MTable

import simpleanonymizer.SlickProfile.api._
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
  private val logger          = LoggerFactory.getLogger(getClass)
  private val sourceDbContext = new DbContext(sourceDb, schema)
  private val targetDbContext = new DbContext(targetDb, schema)
  val tableCopier             = new TableCopier(sourceDbContext, targetDbContext)

  private def copyTablesByLevel(
      levels: Seq[Seq[MTable]],
      specs: Map[String, TableSpec]
  ): Future[Map[String, Int]] = {
    val totalTables = levels.map(_.size).sum
    logger.info(s"Copying $totalTables tables in ${levels.size} levels...")

    levels.foldLeft(Future.successful(Map.empty[String, Int])) { (accFut, level) =>
      accFut.flatMap { acc =>
        // Eagerly create all futures so tables within a level copy in parallel.
        // Future.traverse in Scala 2.13+ is sequential (each future starts only
        // after the previous one completes).
        val futures = level.map { table =>
          val tableName = table.name.name
          if (skippedTables.contains(tableName)) {
            logger.info(s"Skipping table: $tableName")
            Future.successful(tableName -> 0)
          } else
            tableCopier
              .run(tableName, specs.getOrElse(tableName, TableSpec(Seq.empty)))
              .map(count => tableName -> count)
        }
        Future.sequence(futures).map(results => acc ++ results)
      }
    }
  }

  private def addKeysAndSubsetting(
      tableSpecsMap: Map[String, TableSpec],
      pks: Map[String, Set[String]],
      fks: Map[String, Set[String]],
      filters: Map[String, TableSpec.WhereClause]
  ) =
    tableSpecsMap.transform { case (tableName, spec) =>
      val keyColumns =
        (pks.get(tableName) ++ fks.get(tableName)).flatten
          .map(OutputColumn.SourceColumn(_))
      spec.copy(
        columns = (spec.columns ++ keyColumns).distinctBy(_.name),
        whereClause = TableSpec.WhereClause.combine(spec.whereClause, filters.get(tableName))
      )
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
      logicalFks   <- sourceDbContext.logicalForeignKeys
      pks          <- sourceDbContext.allPrimaryKeys
      tableLevels   = TableSorter(tables, fks)
      orderedTables = tableLevels.flatten
      filters       = FilterPropagation.computePropagatedFilters(orderedTables.map(_.name.name), logicalFks)(t => tableSpecsMap.get(t).flatMap(_.whereClause))
      updatedSpecs  = addKeysAndSubsetting(tableSpecsMap, pks, DbContext.fkColumnsByTable(fks), filters)
      _            <- validator.validate(tables.map(_.name.name), skippedTables, updatedSpecs)
      result       <- copyTablesByLevel(tableLevels, updatedSpecs)
    } yield result
  }
}
