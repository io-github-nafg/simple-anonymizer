package simpleanonymizer

import scala.collection.mutable

import slick.jdbc.meta.MForeignKey

/** Filter propagation through FK relationships for database subsetting. */
object FilterPropagation {

  /** Configuration for subsetting a table.
    * @param whereClause
    *   Optional WHERE clause to filter rows (e.g., "created_at > '2024-01-01'")
    * @param skip
    *   If true, skip this table entirely
    * @param copyAll
    *   If true, copy all rows (no subsetting based on FK)
    */
  case class TableConfig(whereClause: Option[String] = None, skip: Boolean = false, copyAll: Boolean = false)

  /** Specification for how to handle a table during snapshot copy.
    *
    * Use the companion object methods to create:
    *   - `TableSpec.skip` - skip this table entirely
    *   - `TableSpec.copy(transformer)` - copy with the given transformer
    *   - `TableSpec.copy(transformer, whereClause)` - copy with filter
    *   - `TableSpec.copyAll(transformer)` - copy all rows, ignore FK filters
    */
  sealed trait TableSpec {
    def transformer: Option[RowTransformer.TableTransformer]
    def config: TableConfig
  }
  object TableSpec       {

    /** Skip this table entirely */
    case object Skip extends TableSpec {
      def transformer: Option[RowTransformer.TableTransformer] = None
      def config: TableConfig                                  = TableConfig(skip = true)
    }

    /** Copy with the given transformer */
    case class Copy(
        t: RowTransformer.TableTransformer,
        whereClause: Option[String] = None,
        copyAll: Boolean = false
    ) extends TableSpec {
      def transformer: Option[RowTransformer.TableTransformer] = Some(t)
      def config: TableConfig                                  = TableConfig(whereClause = whereClause, copyAll = copyAll)
    }

    /** Skip this table entirely */
    def skip: TableSpec = Skip

    /** Copy with the given transformer */
    def copy(transformer: RowTransformer.TableTransformer): TableSpec = Copy(transformer)

    /** Copy with the given transformer and where clause filter */
    def copy(transformer: RowTransformer.TableTransformer, whereClause: String): TableSpec =
      Copy(transformer, whereClause = Some(whereClause))

    /** Copy all rows (ignore FK propagation filters) */
    def copyAll(transformer: RowTransformer.TableTransformer): TableSpec = Copy(transformer, copyAll = true)
  }

  /** Generate a WHERE clause for a child table based on the parent table's filter.
    *
    * For example, if parent = request has filter "created_at > '2024-01-01'" and child = request_field has FK request_field.request_id -> request.id then
    * generates: "request_id IN (SELECT id FROM request WHERE created_at > '2024-01-01')"
    */
  def generateChildWhereClause(
      childTable: String,
      parentFilters: Map[String, String], // parent table -> its WHERE clause
      fks: Seq[MForeignKey]
  ): Option[String] = {
    // Find FKs where this table is the child
    val relevantFks = fks.filter(_.fkTable.name == childTable)

    // For each FK pointing to a parent with a filter, generate a subquery condition
    val conditions = relevantFks.flatMap { fk =>
      parentFilters.get(fk.pkTable.name).map { parentWhere =>
        s"${fk.fkColumn} IN (SELECT ${fk.pkColumn} FROM ${fk.pkTable.name} WHERE $parentWhere)"
      }
    }

    if (conditions.isEmpty) None
    else Some(conditions.mkString(" AND "))
  }

  /** Compute effective WHERE clauses for all tables based on root filters and FK relationships. Propagates filters from parent tables to child tables through
    * the FK graph.
    *
    * @param tables
    *   All tables in insertion order (by level)
    * @param fks
    *   All foreign key relationships
    * @param tableConfigs
    *   User-specified configurations for tables
    * @return
    *   Map of table -> effective WHERE clause
    */
  def computeEffectiveFilters(
      tables: Seq[String],
      fks: Seq[MForeignKey],
      tableConfigs: Map[String, TableConfig]
  ): Map[String, Option[String]] = {
    val effectiveFilters = mutable.Map[String, Option[String]]()

    // Build reverse lookup: child table -> list of FKs
    val fksByChild = fks.groupBy(_.fkTable.name)

    // Process tables in order (parents before children due to level ordering)
    for (table <- tables) {
      val config = tableConfigs.getOrElse(table, TableConfig())

      if (config.skip)
        // Skip this table
        effectiveFilters(table) = None
      else if (config.copyAll)
        // Copy all rows, no filter
        effectiveFilters(table) = None
      else if (config.whereClause.isDefined)
        // User-specified filter takes precedence
        effectiveFilters(table) = config.whereClause
      else {
        // Auto-generate filter based on parent tables
        val parentFilters = effectiveFilters.collect {
          case (t, Some(filter)) if fksByChild.getOrElse(table, Nil).exists(_.pkTable.name == t) => t -> filter
        }.toMap

        effectiveFilters(table) = generateChildWhereClause(table, parentFilters, fks)
      }
    }

    effectiveFilters.toMap
  }
}
