package simpleanonymizer

import scala.language.dynamics

/** Specification for how to handle a table during snapshot copy.
  *
  * @param columns
  *   The columns to SELECT from source and INSERT into target, with optional transformations.
  * @param whereClause
  *   Optional SQL WHERE clause to filter rows during copy.
  * @param limit
  *   Optional row limit (ordered by `id` DESC if an `id` column exists).
  * @param batchSize
  *   Number of rows per INSERT batch (default 1000).
  */
case class TableSpec(
    columns: Seq[OutputColumn],
    whereClause: Option[String] = None,
    limit: Option[Int] = None,
    batchSize: Int = 1000
) {
  private[simpleanonymizer] val columnNames = columns.map(_.name)

  private[simpleanonymizer] def validateCovers(expectedColumns: Set[String]): Either[Set[String], Unit] = {
    val missing = expectedColumns -- columnNames
    if (missing.isEmpty) Right(()) else Left(missing)
  }

  /** Set or replace the WHERE clause for filtering rows. */
  def where(whereClause: String): TableSpec = copy(whereClause = Some(whereClause))

  /** Limit the number of rows copied. */
  def withLimit(n: Int): TableSpec = copy(limit = Some(n))

  /** Set the batch size for INSERT operations. */
  def withBatchSize(n: Int): TableSpec = copy(batchSize = n)
}

object TableSpec {

  /** Dynamic row accessor - allows row.column_name syntax */
  class Row extends Dynamic {
    def selectDynamic(name: String): OutputColumn.SourceColumn = new OutputColumn.SourceColumn(name)
  }

  /** Entry point: select { row => Seq(...) } */
  def select(f: Row => Seq[OutputColumn]): TableSpec =
    TableSpec(
      columns = f(new Row),
      whereClause = None
    )
}
