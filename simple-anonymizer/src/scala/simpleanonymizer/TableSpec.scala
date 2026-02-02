package simpleanonymizer

import scala.language.dynamics

/** Specification for how to handle a table during snapshot copy. */
case class TableSpec(
    columns: Seq[OutputColumn],
    whereClause: Option[String]
) {
  private[simpleanonymizer] val outputColumns: Set[String] = columns.map(_.name).toSet

  private[simpleanonymizer] def validateCovers(expectedColumns: Set[String]): Either[Set[String], Unit] = {
    val missing = expectedColumns -- outputColumns
    if (missing.isEmpty) Right(()) else Left(missing)
  }

  def where(whereClause: String): TableSpec = copy(whereClause = Some(whereClause))
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
