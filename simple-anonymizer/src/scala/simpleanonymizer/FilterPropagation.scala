package simpleanonymizer

import slick.jdbc.meta.MForeignKey

/** Propagates WHERE clauses through foreign key relationships.
  *
  * When a parent table has a filter (e.g., `users WHERE active = true`), child tables that reference it via FK should only include rows that reference the
  * filtered parent rows. This module generates the appropriate subquery-based WHERE clauses.
  */
object FilterPropagation {

  /** Generate a WHERE clause for a child table based on the parent table's filter.
    *
    * @param childTable
    *   Name of the child table
    * @param parentFilters
    *   Map of parent table names to their WHERE clauses
    * @param fks
    *   All foreign key relationships
    * @return
    *   WHERE clause with IN subquery, or None if no parent has a filter
    */
  private[simpleanonymizer] def generateChildWhereClause(
      childTable: String,
      parentFilters: Map[String, String],
      fks: Seq[MForeignKey]
  ): Option[String] = {
    val relevantFks = fks.filter(_.fkTable.name == childTable)
    val conditions  = relevantFks.flatMap { fk =>
      parentFilters.get(fk.pkTable.name).map { parentWhere =>
        s"${fk.fkColumn} IN (SELECT ${fk.pkColumn} FROM ${fk.pkTable.name} WHERE $parentWhere)"
      }
    }
    if (conditions.isEmpty)
      None
    else
      Some(conditions.mkString(" AND "))
  }

  /** Compute effective WHERE clauses for all tables based on root filters and FK relationships.
    *
    * Filters are propagated transitively through the FK graph. If table A has a filter, and table B references A, then B gets an IN subquery filter. If table C
    * references B, it gets a nested filter based on B's filter.
    *
    * @param tables
    *   Tables in topological order (parents before children)
    * @param fks
    *   All foreign key relationships
    * @param tableSpecs
    *   User-provided table specifications (may include explicit WHERE clauses)
    * @return
    *   Map of table name to effective WHERE clause (None if no filter applies)
    */
  def computeEffectiveFilters(
      tables: Seq[String],
      fks: Seq[MForeignKey],
      tableSpecs: Map[String, TableSpec]
  ): Map[String, Option[String]] = {
    val fksByChild = fks.groupBy(_.fkTable.name)

    tables.foldLeft(Map.empty[String, Option[String]]) { (effectiveFilters, table) =>
      val filter = tableSpecs.get(table).flatMap(_.whereClause) match {
        case Some(whereClause) => Some(whereClause)
        case None              =>
          val parentFilters = effectiveFilters.collect {
            case (t, Some(f)) if fksByChild.getOrElse(table, Nil).exists(_.pkTable.name == t) => t -> f
          }
          generateChildWhereClause(table, parentFilters, fks)
      }
      effectiveFilters + (table -> filter)
    }
  }
}
