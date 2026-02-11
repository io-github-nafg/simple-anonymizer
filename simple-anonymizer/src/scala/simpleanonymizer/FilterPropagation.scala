package simpleanonymizer

import scala.annotation.tailrec

import slick.jdbc.meta.{MForeignKey, MQName}

import simpleanonymizer.SlickProfile.quoteIdentifier

/** Propagates WHERE clauses through foreign key relationships.
  *
  * When a parent table has a filter (e.g., `users WHERE active = true`), child tables that reference it via FK should only include rows that reference the
  * filtered parent rows. This module generates the appropriate subquery-based WHERE clauses.
  */
object FilterPropagation {
  private def qualifiedName(mqName: MQName): String =
    mqName.schema match {
      case Some(s) => s"${quoteIdentifier(s)}.${quoteIdentifier(mqName.name)}"
      case None    => quoteIdentifier(mqName.name)
    }

  private def inExpr(fk: MForeignKey, parentClause: TableSpec.WhereClause): TableSpec.WhereClause.Single =
    TableSpec.WhereClause.Single(
      s"${quoteIdentifier(fk.fkColumn)} IN (SELECT ${quoteIdentifier(fk.pkColumn)} FROM ${qualifiedName(fk.pkTable)} WHERE ${parentClause.sql})"
    )

  private def selfRefCteExpr(
      fk: MForeignKey,
      baseFilter: TableSpec.WhereClause
  ): TableSpec.WhereClause.Single = {
    val table     = qualifiedName(fk.fkTable)
    val fkCol     = quoteIdentifier(fk.fkColumn)
    val pkCol     = quoteIdentifier(fk.pkColumn)
    val filterSql = baseFilter.sql
    TableSpec.WhereClause.Single(
      s"($fkCol IS NULL OR $fkCol IN (" +
        s"WITH RECURSIVE reachable($pkCol) AS (" +
        s"SELECT $pkCol FROM $table WHERE ($filterSql) AND $fkCol IS NULL " +
        s"UNION " +
        s"SELECT t.$pkCol FROM $table t JOIN reachable r ON t.$fkCol = r.$pkCol WHERE ($filterSql)" +
        s") SELECT $pkCol FROM reachable))"
    )
  }

  /** Compute propagated WHERE clauses for all tables based on root filters and FK relationships.
    *
    * Filters are propagated transitively through the FK graph. If table A has a filter, and table B references A, then B gets an IN subquery filter. If table C
    * references B, it gets a nested filter based on B's filter.
    *
    * Self-referencing FKs are handled with recursive CTEs: the CTE computes the transitive closure of reachable rows from roots (where the FK column IS NULL)
    * through the self-ref chain, restricted to the filtered set.
    *
    * The returned map contains only the <b>propagated</b> clauses (FK-based IN subqueries). Explicit filters participate in the propagation chain but are not
    * repeated in the output — callers append the propagated clauses to the original explicit filters.
    *
    * @param tables
    *   Tables in topological order (parents before children)
    * @param fks
    *   All foreign key relationships
    * @param explicitClauses
    *   Lookup function returning the user-provided [[TableSpec.WhereClause]] for a table, or None
    * @return
    *   Map of table name to propagated [[TableSpec.WhereClause]] (only tables that received propagated filters)
    */
  def computePropagatedFilters(
      tables: Seq[String],
      fks: Seq[MForeignKey]
  )(explicitClauses: String => Option[TableSpec.WhereClause]): Map[String, TableSpec.WhereClause] = {
    val fksByChild = fks.groupBy(_.fkTable.name).withDefaultValue(Nil)

    @tailrec
    def loop(remaining: List[String], accumulated: Map[String, TableSpec.WhereClause]): Map[String, TableSpec.WhereClause] =
      remaining match {
        case Nil           => accumulated
        case table :: rest =>
          val tableFks    = fksByChild(table)
          val isSelfRef   = (fk: MForeignKey) => fk.pkTable.name == table
          val sortedFks   = tableFks.sortBy(fk => if (isSelfRef(fk)) 1 else 0)
          val whereClause =
            sortedFks
              .foldLeft(Option.empty[TableSpec.WhereClause]) { (acc, fk) =>
                if (isSelfRef(fk)) {
                  val baseFilter = TableSpec.WhereClause.combine(explicitClauses(table), acc)
                  TableSpec.WhereClause.combine(acc, baseFilter.map(selfRefCteExpr(fk, _)))
                } else {
                  val parentEffective = TableSpec.WhereClause.combine(explicitClauses(fk.pkTable.name), accumulated.get(fk.pkTable.name))
                  TableSpec.WhereClause.combine(acc, parentEffective.map(inExpr(fk, _)))
                }
              }
          loop(rest, accumulated ++ whereClause.map(table -> _))
      }

    loop(tables.toList, Map.empty)
  }
}
