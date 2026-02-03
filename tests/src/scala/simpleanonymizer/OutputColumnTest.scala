package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funspec.AnyFunSpec

class OutputColumnTest extends AnyFunSpec with TypeCheckedTripleEquals {
  import OutputColumn._

  private def rawRow(data: Map[String, String]): RawRow =
    RawRow(objects = data.map { case (k, v) => k -> v.asInstanceOf[AnyRef] }, strings = data)

  describe("SourceColumn") {
    it("copies value unchanged") {
      val col       = SourceColumn("name")
      val transform = col.lift(identity)
      val row       = rawRow(Map("name" -> "John"))
      assert(transform(row) === "John")
    }
  }

  describe("FixedColumn") {
    it("returns a fixed string value") {
      val col       = FixedColumn("status", "REDACTED")
      val transform = col.lift(identity)
      val row       = rawRow(Map("status" -> "original"))
      assert(transform(row) === "REDACTED")
    }

    it("returns a numeric value") {
      val col       = FixedColumn("count", 42)
      val transform = col.lift(identity)
      val row       = rawRow(Map("count" -> "999"))
      assert(transform(row).asInstanceOf[Int] === 42)
    }

    it("returns null") {
      val col       = FixedColumn("field", null)
      val transform = col.lift(identity)
      val row       = rawRow(Map("field" -> "some value"))
      assert(transform(row) === null)
    }
  }

  describe("TransformedColumn") {
    it("applies function to string value") {
      val col       = TransformedColumn("name", Lens.Direct, _ => _.map(_.toUpperCase))
      val transform = col.lift(identity)
      val row       = rawRow(Map("name" -> "john"))
      assert(transform(row) === "JOHN")
    }

    it("preserves null values") {
      val col       = TransformedColumn("name", Lens.Direct, _ => _.map(_.toUpperCase))
      val transform = col.lift(identity)
      val row       = RawRow(objects = Map("name" -> null), strings = Map("name" -> null))
      assert(transform(row) === null)
    }

    it("navigates JSON fields") {
      val col       = TransformedColumn("data", Lens.Field("name"), _ => _.map(_.toUpperCase))
      val transform = col.lift(identity)
      val row       = rawRow(Map("data" -> """{"name":"john","age":30}"""))
      assert(transform(row) === """{"name":"JOHN","age":30}""")
    }

    it("navigates JSON arrays") {
      val nav       = Lens.ArrayElements(Lens.Field("number"))
      val col       = TransformedColumn("phones", nav, _ => _.map(_ => "REDACTED"))
      val transform = col.lift(identity)
      val row       = rawRow(Map("phones" -> """[{"type":"home","number":"123"}]"""))
      assert(transform(row) === """[{"type":"home","number":"REDACTED"}]""")
    }
  }

  describe("the wrapIfJson function") {
    it("is applied to transformed values") {
      val col       = TransformedColumn("col", Lens.Direct, _ => _.map(_.toUpperCase))
      val transform = col.lift(v => s"wrapped:$v")
      val row       = rawRow(Map("col" -> "hello"))
      assert(transform(row) === "wrapped:HELLO")
    }

    it("is applied to fixed values") {
      val col       = FixedColumn("col", "value")
      val transform = col.lift(v => s"wrapped:$v")
      val row       = rawRow(Map("col" -> "ignored"))
      assert(transform(row) === "wrapped:value")
    }

    it("is applied to passthrough values") {
      val col       = SourceColumn("col")
      val transform = col.lift(v => s"wrapped:$v")
      val row       = rawRow(Map("col" -> "original"))
      assert(transform(row) === "wrapped:original")
    }
  }
}
