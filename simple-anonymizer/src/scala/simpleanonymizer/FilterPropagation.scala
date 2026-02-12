package simpleanonymizer

import scala.annotation.tailrec

import simpleanonymizer.SlickProfile.{quoteIdentifier, quoteQualified}

/** Propagates WHERE clauses through foreign key relationships.
  *
  * When a parent table has a filter (e.g., `users WHERE active = true`), child tables that reference it via FK should only include rows that reference the
  * filtered parent rows. This module generates the appropriate subquery-based WHERE clauses.
  */
object FilterPropagation {

  private def quotedCols(fk: DbContext.LogicalFK): (Seq[String], Seq[String]) =
    (fk.columns.map(c => quoteIdentifier(c._1)), fk.columns.map(c => quoteIdentifier(c._2)))

  private def sqlTuple(cols: Seq[String]): String =
    if (cols.size == 1) cols.head else cols.mkString("(", ", ", ")")

  private def inSubquery(cols: Seq[String], subquery: String): String =
    s"${sqlTuple(cols)} IN ($subquery)"

  private def recursiveCte(name: String, cols: String, base: String, recursive: String): String =
    s"WITH RECURSIVE $name($cols) AS ($base UNION $recursive) SELECT $cols FROM $name"

  private def inExpr(fk: DbContext.LogicalFK, parentClause: TableSpec.WhereClause): TableSpec.WhereClause.Single = {
    val (fkCols, pkCols) = quotedCols(fk)
    val subquery         = s"SELECT ${pkCols.mkString(", ")} FROM ${quoteQualified(fk.pkTable)} WHERE ${parentClause.sql}"
    TableSpec.WhereClause.Single(inSubquery(fkCols, subquery))
  }

  private def selfRefCteExpr(
      fk: DbContext.LogicalFK,
      baseFilter: TableSpec.WhereClause
  ): TableSpec.WhereClause.Single = {
    val table            = quoteQualified(fk.fkTable)
    val (fkCols, pkCols) = quotedCols(fk)
    val cteName          = s"_reachable_${fk.fkTable.name}"
    val cteCols          = fk.columns.map(c => quoteIdentifier(s"_r_${c._2}"))
    val cteColList       = cteCols.mkString(", ")
    val nullCheck        = fkCols.map(c => s"$c IS NULL").mkString(" AND ")
    val joinCond         = fkCols.zip(cteCols).map { case (fc, cc) => s"t.$fc = r.$cc" }.mkString(" AND ")
    val filterSql        = baseFilter.sql
    val cte              = recursiveCte(
      cteName,
      cteColList,
      base = s"SELECT ${pkCols.mkString(", ")} FROM $table WHERE ($filterSql) AND $nullCheck",
      recursive = s"SELECT ${pkCols.map(c => s"t.$c").mkString(", ")} FROM $table t JOIN $cteName r ON $joinCond WHERE ($filterSql)"
    )
    TableSpec.WhereClause.Single(s"($nullCheck OR ${inSubquery(fkCols, cte)})")
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
    *   Logical foreign key constraints (composite columns pre-grouped)
    * @param explicitClauses
    *   Lookup function returning the user-provided [[TableSpec.WhereClause]] for a table, or None
    * @return
    *   Map of table name to propagated [[TableSpec.WhereClause]] (only tables that received propagated filters)
    */
  def computePropagatedFilters(
      tables: Seq[String],
      fks: Seq[DbContext.LogicalFK]
  )(explicitClauses: String => Option[TableSpec.WhereClause]): Map[String, TableSpec.WhereClause] = {
    val fksByChild = fks.groupBy(_.fkTable.name).withDefaultValue(Nil)

    @tailrec
    def loop(remaining: List[String], accumulated: Map[String, TableSpec.WhereClause]): Map[String, TableSpec.WhereClause] =
      remaining match {
        case Nil           => accumulated
        case table :: rest =>
          val (selfRefFks, crossRefFks) = fksByChild(table).partition(_.isSelfRef)
          val crossRefClause            =
            crossRefFks.foldLeft(Option.empty[TableSpec.WhereClause]) { (acc, fk) =>
              val parentEffective = TableSpec.WhereClause.combine(explicitClauses(fk.pkTable.name), accumulated.get(fk.pkTable.name))
              TableSpec.WhereClause.combine(acc, parentEffective.map(inExpr(fk, _)))
            }
          val baseFilter                = TableSpec.WhereClause.combine(explicitClauses(table), crossRefClause)
          val whereClause               =
            selfRefFks.foldLeft(crossRefClause) { (acc, fk) =>
              TableSpec.WhereClause.combine(acc, baseFilter.map(selfRefCteExpr(fk, _)))
            }
          loop(rest, accumulated ++ whereClause.map(table -> _))
      }

    loop(tables.toList, Map.empty)
  }
}
