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

  /** A logical FK constraint, possibly composite (multiple column pairs). */
  private case class LogicalFK(
      fkTable: MQName,
      pkTable: MQName,
      columns: Seq[(String, String)] // (fkColumn, pkColumn) pairs
  ) {
    def isSelfRef: Boolean = pkTable.name == fkTable.name
  }

  private def groupFKs(fks: Seq[MForeignKey]): Seq[LogicalFK] = {
    def toLogicalFK(group: Seq[MForeignKey]): LogicalFK = {
      val sorted = group.sortBy(_.keySeq)
      LogicalFK(sorted.head.fkTable, sorted.head.pkTable, sorted.map(fk => (fk.fkColumn, fk.pkColumn)))
    }
    val (named, unnamed)                                = fks.partition(_.fkName.isDefined)
    val namedGroups                                     = named.groupBy(fk => (fk.fkTable.name, fk.pkTable.name, fk.fkName)).values.map(toLogicalFK)
    val unnamedGroups                                   = unnamed.map(fk => toLogicalFK(Seq(fk)))
    (namedGroups ++ unnamedGroups).toSeq
  }

  private def quotedCols(fk: LogicalFK): (Seq[String], Seq[String]) =
    (fk.columns.map(c => quoteIdentifier(c._1)), fk.columns.map(c => quoteIdentifier(c._2)))

  private def sqlTuple(cols: Seq[String]): String =
    if (cols.size == 1) cols.head else cols.mkString("(", ", ", ")")

  private def inExpr(fk: LogicalFK, parentClause: TableSpec.WhereClause): TableSpec.WhereClause.Single = {
    val (fkCols, pkCols) = quotedCols(fk)
    TableSpec.WhereClause.Single(
      s"${sqlTuple(fkCols)} IN (SELECT ${pkCols.mkString(", ")} FROM ${qualifiedName(fk.pkTable)} WHERE ${parentClause.sql})"
    )
  }

  private def selfRefCteExpr(
      fk: LogicalFK,
      baseFilter: TableSpec.WhereClause
  ): TableSpec.WhereClause.Single = {
    val table            = qualifiedName(fk.fkTable)
    val (fkCols, pkCols) = quotedCols(fk)
    val pkColList        = pkCols.mkString(", ")
    val nullCheck        = fkCols.map(c => s"$c IS NULL").mkString(" AND ")
    val joinCond         = fkCols.zip(pkCols).map { case (fc, pc) => s"t.$fc = r.$pc" }.mkString(" AND ")
    val filterSql        = baseFilter.sql
    TableSpec.WhereClause.Single(
      s"($nullCheck OR ${sqlTuple(fkCols)} IN (" +
        s"WITH RECURSIVE reachable($pkColList) AS (" +
        s"SELECT $pkColList FROM $table WHERE ($filterSql) AND $nullCheck " +
        s"UNION " +
        s"SELECT ${pkCols.map(c => s"t.$c").mkString(", ")} FROM $table t JOIN reachable r ON $joinCond WHERE ($filterSql)" +
        s") SELECT $pkColList FROM reachable))"
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
    val logicalFks = groupFKs(fks)
    val fksByChild = logicalFks.groupBy(_.fkTable.name).withDefaultValue(Nil)

    @tailrec
    def loop(remaining: List[String], accumulated: Map[String, TableSpec.WhereClause]): Map[String, TableSpec.WhereClause] =
      remaining match {
        case Nil           => accumulated
        case table :: rest =>
          val tableFks    = fksByChild(table)
          val sortedFks   = tableFks.sortBy(fk => if (fk.isSelfRef) 1 else 0)
          val whereClause =
            sortedFks
              .foldLeft(Option.empty[TableSpec.WhereClause]) { (acc, fk) =>
                if (fk.isSelfRef) {
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
