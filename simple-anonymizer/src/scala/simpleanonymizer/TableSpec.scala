package simpleanonymizer

import scala.language.dynamics

/** Specification for how to handle a table during snapshot copy.
  *
  * @param columns
  *   The columns to SELECT from source and INSERT into target, with optional transformations.
  * @param whereClause
  *   Optional filter for rows during copy. Built via `.where(...)` calls, which are ANDed together into a [[TableSpec.WhereClause]] tree.
  * @param limit
  *   Optional row limit (ordered by `id` DESC if an `id` column exists).
  * @param batchSize
  *   Number of rows per INSERT batch (default 1000).
  * @param onConflict
  *   Behavior when INSERT conflicts with existing data (default: fail on conflict).
  */
case class TableSpec(
    columns: Seq[OutputColumn],
    whereClause: Option[TableSpec.WhereClause] = None,
    limit: Option[Int] = None,
    batchSize: Int = 1000,
    onConflict: Option[OnConflict] = None
) {
  private[simpleanonymizer] val columnNames = columns.map(_.name)

  private[simpleanonymizer] def validateCovers(expectedColumns: Set[String]): Either[Set[String], Unit] = {
    val missing = expectedColumns -- columnNames
    if (missing.isEmpty) Right(()) else Left(missing)
  }

  /** Add a WHERE clause for filtering rows. Multiple calls are ANDed together. */
  def where(whereClause: String): TableSpec =
    copy(whereClause = TableSpec.WhereClause.combine(this.whereClause, Some(TableSpec.WhereClause.Single(whereClause))))

  /** Limit the number of rows copied. */
  def withLimit(n: Int): TableSpec = copy(limit = Some(n))

  /** Set the batch size for INSERT operations. */
  def withBatchSize(n: Int): TableSpec = copy(batchSize = n)

  /** Set the ON CONFLICT strategy for handling duplicate rows during INSERT. */
  def onConflict(strategy: OnConflict): TableSpec = copy(onConflict = Some(strategy))
}

object TableSpec {
  sealed trait WhereClause {
    def sql: String
    def clauses: Seq[String]
    def and(other: WhereClause): WhereClause
    final def and(other: String): WhereClause = and(WhereClause.Single(other))
  }
  object WhereClause       {
    case class Single(sql: String)                       extends WhereClause {
      override def clauses: Seq[String]                 = Seq(sql)
      override def and(other: WhereClause): WhereClause = Multiple(sql, other.clauses)
    }
    case class Multiple(head: String, tail: Seq[String]) extends WhereClause {
      override def clauses: Seq[String]                 = head +: tail
      override def sql: String                          = clauses.map("(" + _ + ")").mkString(" AND ")
      override def and(other: WhereClause): WhereClause = Multiple(head, tail ++ other.clauses)
    }

    def combine(a: Option[WhereClause], b: Option[WhereClause]): Option[WhereClause] = (a, b) match {
      case (None, None)       => None
      case (None, Some(b))    => Some(b)
      case (Some(a), None)    => Some(a)
      case (Some(a), Some(b)) => Some(a.and(b))
    }
  }

  /** Dynamic row accessor - allows row.column_name syntax */
  class Row extends Dynamic {
    def selectDynamic(name: String): OutputColumn.SourceColumn = new OutputColumn.SourceColumn(name)
  }

  /** Entry point: select { row => Seq(...) } */
  def select(f: Row => Seq[OutputColumn]): TableSpec =
    TableSpec(
      columns = f(new Row)
    )
}
