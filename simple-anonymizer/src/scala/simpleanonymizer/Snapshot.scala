package simpleanonymizer

import scala.concurrent.{ExecutionContext, Future}

/** High-level orchestrator for copying a database snapshot with subsetting and anonymization.
  *
  * Requires explicit handling for all tables - either a transformer or explicit skip. This ensures every column is consciously handled.
  *
  * @example
  *   {{{
  * import simpleanonymizer.DbSnapshot.TableSpec
  * import simpleanonymizer.DbSnapshot.TableSpec._
  *
  * val snapshot = new Snapshot(sourceDb, targetDb)
  *
  * // Preferred API: single map with TableSpec
  * for {
  *   result <- snapshot.copy(Map(
  *     "users"      -> copy(table("first_name" -> using(FirstName.anonymize), ...), whereClause = "active = true"),
  *     "orders"     -> copy(table("description" -> passthrough, ...)),
  *     "audit_logs" -> skip
  *   ))
  * } yield result  // Map[tableName -> rowCount]
  *
  * // Alternative API: separate tableConfigs and transformers maps
  * for {
  *   result <- snapshot.copy(
  *     tableConfigs = Map("audit_logs" -> TableConfig(skip = true)),
  *     transformers = Map("users" -> table(...), "orders" -> table(...))
  *   )
  * } yield result
  *   }}}
  */
class Snapshot(sourceDb: SlickProfile.api.Database, targetDb: SlickProfile.api.Database, schema: String = "public")(implicit
    ec: ExecutionContext
) {
  import DbSnapshot._
  import SlickProfile.api.DBIO

  /** Copy all tables from source to target with transformation (preferred API).
    *
    * Tables are copied in topological order based on FK dependencies. Filters are automatically propagated from parent tables to child tables through FK
    * relationships.
    *
    * @param tableSpecs
    *   Specification for each table: `skip` to exclude, `copy(transformer)` to include
    * @return
    *   Map of table name to number of rows copied
    * @throws IllegalArgumentException
    *   if any table in the database is not specified, or if a transformer is missing columns. Error messages include copy-pastable code snippets.
    */
  def copy(tableSpecs: Map[String, TableSpec]): Future[Map[String, Int]] = {
    val tableConfigs = tableSpecs.view.mapValues(_.config).toMap
    val transformers = tableSpecs.collect { case (name, spec) if spec.transformer.isDefined => name -> spec.transformer.get }
    copy(tableConfigs, transformers)
  }

  /** Copy all tables from source to target with transformation.
    *
    * Tables are copied in topological order based on FK dependencies. Filters are automatically propagated from parent tables to child tables through FK
    * relationships.
    *
    * @param tableConfigs
    *   Configuration for each table (where clause, skip, copyAll)
    * @param transformers
    *   Row transformers for all non-skipped tables (required)
    * @return
    *   Map of table name to number of rows copied
    * @throws IllegalArgumentException
    *   if any non-skipped table is missing a transformer, or if a transformer is missing columns. Error messages include copy-pastable code snippets.
    */
  def copy(
      tableConfigs: Map[String, TableConfig],
      transformers: Map[String, RowTransformer.TableTransformer]
  ): Future[Map[String, Int]] = {
    val skippedTables = tableConfigs.collect { case (table, config) if config.skip => table }.toSet

    for {
      tables       <- sourceDb.run(getAllTables(schema))
      fks          <- sourceDb.run(getForeignKeys(schema))
      tableLevels   = computeTableLevels(tables, fks)
      orderedGroups = groupTablesByLevel(tableLevels)
      orderedTables = orderedGroups.flatten
      filters       = computeEffectiveFilters(orderedTables, fks, tableConfigs)
      // Validate that all non-skipped tables have transformers
      _            <- validateTableCoverage(tables, skippedTables, transformers)
      // Validate that each transformer covers all required columns
      _            <- validateAllTransformers(transformers)
      // Copy tables level by level
      result       <- copyTablesInOrder(orderedGroups.flatten, skippedTables, transformers, filters)
    } yield result
  }

  private def validateTableCoverage(
      tables: Seq[String],
      skippedTables: Set[String],
      transformers: Map[String, RowTransformer.TableTransformer]
  ): Future[Unit] = {
    val requiredTables = tables.filterNot(skippedTables.contains)
    val missingTables  = requiredTables.filterNot(transformers.contains)

    if (missingTables.nonEmpty) {
      // Generate helpful snippets for missing tables
      val columnQueries: Seq[DBIO[(String, Seq[String])]] = missingTables.map { table =>
        getDataColumns(table, schema).map(cols => table -> cols)
      }
      sourceDb
        .run(DBIO.sequence(columnQueries))
        .flatMap { tableColumns =>
          val snippets = tableColumns.map { case (table, cols) =>
            generateTableSnippet(table, cols)
          }

          val errorMsg = new StringBuilder
          errorMsg.append(s"Missing transformers for ${missingTables.size} table(s).\n\n")
          errorMsg.append("Add these tables to 'transformers':\n\n")
          snippets.foreach { snippet =>
            errorMsg.append(snippet)
            errorMsg.append(",\n\n")
          }
          errorMsg.append("Or mark them as skipped in 'tableConfigs':\n")
          missingTables.foreach { t =>
            errorMsg.append(s""""$t" -> TableConfig(skip = true),\n""")
          }

          Future.failed(new IllegalArgumentException(errorMsg.toString))
        }
    } else
      Future.successful(())
  }

  private def validateAllTransformers(
      transformers: Map[String, RowTransformer.TableTransformer]
  ): Future[Unit] = {
    val validations: Seq[Future[(String, Either[Set[String], Unit])]] = transformers.toSeq.map { case (tableName, transformer) =>
      sourceDb.run(validateTransformerCoverage(tableName, transformer, schema)).map { result =>
        (tableName, result)
      }
    }

    Future.sequence(validations).flatMap { results =>
      val failures = results.collect { case (tableName, Left(missing)) => (tableName, missing) }

      if (failures.nonEmpty) {
        val errorMsg = new StringBuilder
        errorMsg.append(s"Transformers are missing columns for ${failures.size} table(s).\n\n")

        failures.foreach { case (tableName, missing) =>
          errorMsg.append(s"Table '$tableName' is missing ${missing.size} column(s). Add these:\n")
          errorMsg.append("    ")
          errorMsg.append(generateColumnSnippets(missing))
          errorMsg.append("\n\n")
        }

        Future.failed(new IllegalArgumentException(errorMsg.toString))
      } else
        Future.successful(())
    }
  }

  private def copyTablesInOrder(
      tables: Seq[String],
      skippedTables: Set[String],
      transformers: Map[String, RowTransformer.TableTransformer],
      filters: Map[String, Option[String]]
  ): Future[Map[String, Int]] = {
    val results = scala.collection.mutable.Map[String, Int]()

    def copyNext(remaining: List[String]): Future[Map[String, Int]] =
      remaining match {
        case Nil           => Future.successful(results.toMap)
        case table :: rest =>
          if (skippedTables.contains(table)) {
            println(s"[Snapshot] Skipping table: $table")
            results(table) = 0
            copyNext(rest)
          } else
            for {
              columns <- sourceDb.run(getTableColumns(table, schema))
              count   <- copyTable(
                           sourceDb = sourceDb,
                           targetDb = targetDb,
                           tableName = table,
                           columns = columns,
                           whereClause = filters.getOrElse(table, None),
                           transformer = transformers.get(table)
                         )
              _        = results(table) = count
              result  <- copyNext(rest)
            } yield result
      }

    copyNext(tables.toList)
  }
}
