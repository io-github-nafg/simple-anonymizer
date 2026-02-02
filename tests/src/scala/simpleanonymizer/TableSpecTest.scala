package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funsuite.AnyFunSuite

class TableSpecTest extends AnyFunSuite with TypeCheckedTripleEquals {

  // ============================================================================
  // DSL: TableSpec.select
  // ============================================================================

  test("passthrough creates SourceColumn") {
    val plans = TableSpec.select(row => Seq(row.col)).columns
    assert(plans.head.isInstanceOf[OutputColumn.SourceColumn])
  }

  test("nulled creates FixedColumn with null value") {
    val plans = TableSpec.select(row => Seq(row.col.nulled)).columns
    plans.head match {
      case OutputColumn.FixedColumn(_, v) => assert(v === null)
      case other                          => fail(s"Expected Fixed(null), got $other")
    }
  }

  test("fixed creates Fixed OutputColumn with correct value") {
    val plans = TableSpec.select(row => Seq(row.col := 42)).columns
    plans.head match {
      case OutputColumn.FixedColumn(_, v) => assert(v === 42)
      case other                          => fail(s"Expected Fixed, got $other")
    }
  }

  test("mapString creates TransformedColumn") {
    val plans    = TableSpec.select(row => Seq(row.col.mapString(_.toUpperCase))).columns
    val dummyRow = RawRow(Map.empty, Map.empty)
    plans.head match {
      case OutputColumn.TransformedColumn(_, nav, f) =>
        assert(nav == Lens.Direct)
        assert(f(dummyRow)(Some("hello")) === Some("HELLO"))
      case other                                     => fail(s"Expected Transform, got $other")
    }
  }

  test("mapJsonArray creates Transform OutputColumn with JSON navigation") {
    val plans    = TableSpec.select(row => Seq(row.phones.mapJsonArray(_.number.mapString(_ => "XXX")))).columns
    val dummyRow = RawRow(Map.empty, Map.empty)
    plans.head match {
      case OutputColumn.TransformedColumn(_, nav, f) =>
        assert(nav.isInstanceOf[Lens.ArrayElements])
        assert(f(dummyRow)(Some("123")) === Some("XXX"))
      case other                                     => fail(s"Expected Transform, got $other")
    }
  }

  test("mapString preserves null (function not called)") {
    val plans    =
      TableSpec.select { row =>
        Seq(row.col.mapString(_ => fail("should not be called")))
      }.columns
    val dummyRow = RawRow(Map.empty, Map.empty)
    plans.head match {
      case OutputColumn.TransformedColumn(_, _, f) =>
        assert(f(dummyRow)(None) === None)
      case other                                   => fail(s"Expected Transform, got $other")
    }
  }

  test("mapOptString passes None for null values") {
    var received: Option[String] = Some("not called")
    val plans                    = TableSpec
      .select(row =>
        Seq(row.col.mapOptString { opt =>
          received = opt
          Some("replaced")
        })
      )
      .columns
    val dummyRow                 = RawRow(Map.empty, Map.empty)
    plans.head match {
      case OutputColumn.TransformedColumn(_, _, f) =>
        assert(f(dummyRow)(None) === Some("replaced"))
        assert(received === None)
      case other                                   => fail(s"Expected Transform, got $other")
    }
  }

  test("Seq combines multiple columns") {
    val plans = TableSpec.select(row => Seq(row.a, row.b, row.c)).columns
    assert(plans.size === 3)
    assert(plans.map(_.name) === Seq("a", "b", "c"))
  }

  // ============================================================================
  // TableSpec validation
  // ============================================================================

  test("outputColumns returns column names from all plans") {
    val spec = TableSpec(
      columns = Seq(
        OutputColumn.SourceColumn("a"),
        OutputColumn.FixedColumn("b", 42),
        OutputColumn.SourceColumn("c")
      ),
      whereClause = None
    )
    assert(spec.outputColumns === Set("a", "b", "c"))
  }

  test("validateCovers returns Right when all columns covered") {
    val spec = TableSpec(
      columns = Seq(
        OutputColumn.SourceColumn("a"),
        OutputColumn.SourceColumn("b")
      ),
      whereClause = None
    )
    assert(spec.validateCovers(Set("a", "b")) === Right(()))
  }

  test("validateCovers returns Left with missing columns") {
    val spec = TableSpec(
      columns = Seq(
        OutputColumn.SourceColumn("a")
      ),
      whereClause = None
    )
    assert(spec.validateCovers(Set("a", "b", "c")) === Left(Set("b", "c")))
  }

  test("validateCovers returns Right when spec has extra columns") {
    val spec = TableSpec(
      columns = Seq(
        OutputColumn.SourceColumn("a"),
        OutputColumn.SourceColumn("b"),
        OutputColumn.SourceColumn("c")
      ),
      whereClause = None
    )
    assert(spec.validateCovers(Set("a", "b")) === Right(()))
  }
}
