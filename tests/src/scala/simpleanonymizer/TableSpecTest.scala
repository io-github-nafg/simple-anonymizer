package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funspec.AnyFunSpec

class TableSpecTest extends AnyFunSpec with TypeCheckedTripleEquals {

  describe("TableSpec.select DSL") {
    it("creates SourceColumn for passthrough") {
      val plans = TableSpec.select(row => Seq(row.col)).columns
      assert(plans.head.isInstanceOf[OutputColumn.SourceColumn])
    }

    it("creates FixedColumn with null for .nulled") {
      val plans = TableSpec.select(row => Seq(row.col.nulled)).columns
      plans.head match {
        case OutputColumn.FixedColumn(_, v) => assert(v === null)
        case other                          => fail(s"Expected Fixed(null), got $other")
      }
    }

    it("creates FixedColumn with correct value for :=") {
      val plans = TableSpec.select(row => Seq(row.col := 42)).columns
      plans.head match {
        case OutputColumn.FixedColumn(_, v) => assert(v === 42)
        case other                          => fail(s"Expected Fixed, got $other")
      }
    }

    it("creates TransformedColumn for .mapString") {
      val plans    = TableSpec.select(row => Seq(row.col.mapString(_.toUpperCase))).columns
      val dummyRow = RawRow(Map.empty, Map.empty)
      plans.head match {
        case OutputColumn.TransformedColumn(_, nav, f) =>
          assert(nav == Lens.Direct)
          assert(f(dummyRow)(Some("hello")) === Some("HELLO"))
        case other                                     => fail(s"Expected Transform, got $other")
      }
    }

    it("creates TransformedColumn with JSON navigation for .mapJsonArray") {
      val plans    = TableSpec.select(row => Seq(row.phones.mapJsonArray(_.number.mapString(_ => "XXX")))).columns
      val dummyRow = RawRow(Map.empty, Map.empty)
      plans.head match {
        case OutputColumn.TransformedColumn(_, nav, f) =>
          assert(nav.isInstanceOf[Lens.ArrayElements])
          assert(f(dummyRow)(Some("123")) === Some("XXX"))
        case other                                     => fail(s"Expected Transform, got $other")
      }
    }

    it("preserves null in .mapString without calling the function") {
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

    it("passes None for null values in .mapOptString") {
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

    it("combines multiple columns from a Seq") {
      val plans = TableSpec.select(row => Seq(row.a, row.b, row.c)).columns
      assert(plans.size === 3)
      assert(plans.map(_.name) === Seq("a", "b", "c"))
    }
  }

  describe("validateCovers") {
    it("returns Right when all columns are covered") {
      val spec = TableSpec(
        columns = Seq(
          OutputColumn.SourceColumn("a"),
          OutputColumn.SourceColumn("b")
        ),
        whereClause = None
      )
      assert(spec.validateCovers(Set("a", "b")) === Right(()))
    }

    it("returns Left with missing columns") {
      val spec = TableSpec(
        columns = Seq(
          OutputColumn.SourceColumn("a")
        ),
        whereClause = None
      )
      assert(spec.validateCovers(Set("a", "b", "c")) === Left(Set("b", "c")))
    }

    it("returns Right when spec has extra columns") {
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

  describe("outputColumns") {
    it("returns column names from all plans") {
      val spec = TableSpec(
        columns = Seq(
          OutputColumn.SourceColumn("a"),
          OutputColumn.FixedColumn("b", 42),
          OutputColumn.SourceColumn("c")
        ),
        whereClause = None
      )
      assert(spec.columnNames === Seq("a", "b", "c"))
    }
  }
}
