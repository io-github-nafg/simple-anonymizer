package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funsuite.AnyFunSuite

class OutputColumnTest extends AnyFunSuite with TypeCheckedTripleEquals {
  import OutputColumn._

  // Helper to create RawRow from a simple string map
  private def rawRow(data: Map[String, String]): RawRow =
    RawRow(objects = data.map { case (k, v) => k -> v.asInstanceOf[AnyRef] }, strings = data)

  // ============================================================================
  // SourceColumn (passthrough) tests
  // ============================================================================

  test("SourceColumn copies value unchanged") {
    val col       = SourceColumn("name")
    val transform = col.transform(identity)
    val row       = rawRow(Map("name" -> "John"))
    assert(transform(row) === "John")
  }

  // ============================================================================
  // FixedColumn tests
  // ============================================================================

  test("FixedColumn returns the fixed string value") {
    val col       = FixedColumn("status", "REDACTED")
    val transform = col.transform(identity)
    val row       = rawRow(Map("status" -> "original"))
    assert(transform(row) === "REDACTED")
  }

  test("FixedColumn returns numeric value") {
    val col       = FixedColumn("count", 42)
    val transform = col.transform(identity)
    val row       = rawRow(Map("count" -> "999"))
    assert(transform(row).asInstanceOf[Int] === 42)
  }

  test("FixedColumn returns null value") {
    val col       = FixedColumn("field", null)
    val transform = col.transform(identity)
    val row       = rawRow(Map("field" -> "some value"))
    assert(transform(row) === null)
  }

  // ============================================================================
  // TransformedColumn tests
  // ============================================================================

  test("TransformedColumn applies function to string value") {
    val col       = TransformedColumn("name", Lens.Direct, _ => _.map(_.toUpperCase))
    val transform = col.transform(identity)
    val row       = rawRow(Map("name" -> "john"))
    assert(transform(row) === "JOHN")
  }

  test("TransformedColumn preserves null values") {
    val col       = TransformedColumn("name", Lens.Direct, _ => _.map(_.toUpperCase))
    val transform = col.transform(identity)
    val row       = RawRow(objects = Map("name" -> null), strings = Map("name" -> null))
    assert(transform(row) === null)
  }

  test("TransformedColumn with JSON field navigation") {
    val col       = TransformedColumn("data", Lens.Field("name"), _ => _.map(_.toUpperCase))
    val transform = col.transform(identity)
    val row       = rawRow(Map("data" -> """{"name":"john","age":30}"""))
    assert(transform(row) === """{"name":"JOHN","age":30}""")
  }

  test("TransformedColumn with JSON array navigation") {
    val nav       = Lens.ArrayElements(Lens.Field("number"))
    val col       = TransformedColumn("phones", nav, _ => _.map(_ => "REDACTED"))
    val transform = col.transform(identity)
    val row       = rawRow(Map("phones" -> """[{"type":"home","number":"123"}]"""))
    assert(transform(row) === """[{"type":"home","number":"REDACTED"}]""")
  }

  // ============================================================================
  // Wrapper function tests
  // ============================================================================

  test("wrapIfJson function is applied to transformed values") {
    val col       = TransformedColumn("col", Lens.Direct, _ => _.map(_.toUpperCase))
    val transform = col.transform(v => s"wrapped:$v")
    val row       = rawRow(Map("col" -> "hello"))
    assert(transform(row) === "wrapped:HELLO")
  }

  test("wrapIfJson function is applied to fixed values") {
    val col       = FixedColumn("col", "value")
    val transform = col.transform(v => s"wrapped:$v")
    val row       = rawRow(Map("col" -> "ignored"))
    assert(transform(row) === "wrapped:value")
  }

  test("wrapIfJson function is applied to passthrough values") {
    val col       = SourceColumn("col")
    val transform = col.transform(v => s"wrapped:$v")
    val row       = rawRow(Map("col" -> "original"))
    assert(transform(row) === "wrapped:original")
  }
}
