package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funsuite.AnyFunSuite

class RowTransformerTest extends AnyFunSuite with TypeCheckedTripleEquals {
  import RowTransformer._
  import RowTransformer.DSL._

  // ============================================================================
  // JsonNav tests
  // ============================================================================

  test("JsonNav.Field transforms a specific field in JSON object") {
    val nav     = JsonNav.Field("name")
    val wrapped = nav.wrap(_.toUpperCase)
    val result  = wrapped("""{"name":"john","age":30}""")
    assert(result === """{"name":"JOHN","age":30}""")
  }

  test("JsonNav.Field leaves other fields unchanged") {
    val nav     = JsonNav.Field("name")
    val wrapped = nav.wrap(_ => "REPLACED")
    val result  = wrapped("""{"name":"john","city":"NYC"}""")
    assert(result === """{"name":"REPLACED","city":"NYC"}""")
  }

  test("JsonNav.ArrayOf transforms each element in array") {
    val nav     = JsonNav.ArrayOf(JsonNav.Direct)
    val wrapped = nav.wrap(_.toUpperCase)
    val result  = wrapped("""["a","b","c"]""")
    assert(result === """["A","B","C"]""")
  }

  test("JsonNav.ArrayOf with Field transforms nested field in each element") {
    val nav     = JsonNav.ArrayOf(JsonNav.Field("number"))
    val wrapped = nav.wrap(_ => "XXX")
    val input   = """[{"type":"home","number":"123"},{"type":"work","number":"456"}]"""
    val result  = wrapped(input)
    assert(result === """[{"type":"home","number":"XXX"},{"type":"work","number":"XXX"}]""")
  }

  // ============================================================================
  // DSL tests
  // ============================================================================

  test("passthrough copies value unchanged") {
    val transformer = table("name" -> passthrough)
    val row         = Map("name" -> "John")
    val result      = transformer.transform(row)
    assert(result("name") === "John")
  }

  test("using applies transformation function") {
    val transformer = table("name" -> using(_.toUpperCase))
    val row         = Map("name" -> "John")
    val result      = transformer.transform(row)
    assert(result("name") === "JOHN")
  }

  test("jsonArray transforms field in each JSON array element") {
    val transformer = table(
      "phones" -> jsonArray("number")(_ => "REDACTED")
    )
    val row         = Map("phones" -> """[{"type":"home","number":"123"}]""")
    val result      = transformer.transform(row)
    assert(result("phones") === """[{"type":"home","number":"REDACTED"}]""")
  }

  // ============================================================================
  // Dependent columns tests
  // ============================================================================

  test("col creates single column dependency") {
    val transformer = table(
      "gender"   -> passthrough,
      "greeting" -> col("gender").map(g => _ => if (g == "M") "Mr." else "Ms.")
    )
    val row         = Map("gender" -> "M", "greeting" -> "")
    val result      = transformer.transform(row)
    assert(result("greeting") === "Mr.")
  }

  test("col.and creates two column dependency") {
    val transformer = table(
      "first" -> passthrough,
      "last"  -> passthrough,
      "full"  -> col("first").and(col("last")).map((f, l) => _ => s"$f $l")
    )
    val row         = Map("first" -> "John", "last" -> "Doe", "full" -> "")
    val result      = transformer.transform(row)
    assert(result("full") === "John Doe")
  }

  test("col.and.and creates three column dependency") {
    val transformer = table(
      "a"        -> passthrough,
      "b"        -> passthrough,
      "c"        -> passthrough,
      "combined" -> col("a").and(col("b")).and(col("c")).map((a, b, c) => _ => s"$a-$b-$c")
    )
    val row         = Map("a" -> "1", "b" -> "2", "c" -> "3", "combined" -> "")
    val result      = transformer.transform(row)
    assert(result("combined") === "1-2-3")
  }

  // ============================================================================
  // TableTransformer tests
  // ============================================================================

  test("TableTransformer transforms all specified columns") {
    val transformer = table(
      "first" -> using(_.toUpperCase),
      "last"  -> using(_.toLowerCase)
    )
    val row         = Map("first" -> "John", "last" -> "DOE")
    val result      = transformer.transform(row)
    assert(result("first") === "JOHN")
    assert(result("last") === "doe")
  }

  test("TableTransformer.columnNames returns correct set") {
    val transformer = table(
      "a" -> passthrough,
      "b" -> passthrough,
      "c" -> passthrough
    )
    assert(transformer.columnNames === Set("a", "b", "c"))
  }

  test("validateCovers returns Right when all columns covered") {
    val transformer = table(
      "a" -> passthrough,
      "b" -> passthrough
    )
    assert(transformer.validateCovers(Set("a", "b")) === Right(()))
  }

  test("validateCovers returns Left with missing columns") {
    val transformer = table(
      "a" -> passthrough
    )
    assert(transformer.validateCovers(Set("a", "b", "c")) === Left(Set("b", "c")))
  }

  // ============================================================================
  // Integration tests
  // ============================================================================

  test("full table transformer with passthrough, using, and jsonArray") {
    val transformer = table(
      "id"     -> passthrough,
      "name"   -> using(_.toUpperCase),
      "phones" -> jsonArray("number")(_ => "XXX-XXX-XXXX")
    )
    val row         = Map(
      "id"     -> "123",
      "name"   -> "John",
      "phones" -> """[{"type":"home","number":"555-1234"}]"""
    )
    val result      = transformer.transform(row)
    assert(result("id") === "123")
    assert(result("name") === "JOHN")
    assert(result("phones") === """[{"type":"home","number":"XXX-XXX-XXXX"}]""")
  }

  test("dependent column transforms based on another column value") {
    import DeterministicAnonymizer._
    val transformer = table(
      "gender"     -> passthrough,
      "first_name" -> col("gender").map {
        case "M" => MaleFirstName.anonymize
        case "F" => FemaleFirstName.anonymize
        case _   => FirstName.anonymize
      }
    )
    val maleRow     = Map("gender" -> "M", "first_name" -> "OriginalName")
    val femaleRow   = Map("gender" -> "F", "first_name" -> "OriginalName")

    val maleResult   = transformer.transform(maleRow)
    val femaleResult = transformer.transform(femaleRow)

    // Results should be different because different name lists are used
    assert(maleResult("first_name").nonEmpty)
    assert(femaleResult("first_name").nonEmpty)
  }

  // ============================================================================
  // ColumnPlan type tests
  // ============================================================================

  test("passthrough creates Passthrough ColumnPlan") {
    val plan = passthrough.bindTo("col")
    assert(plan.isInstanceOf[ColumnPlan.Passthrough])
  }

  test("setNull creates SetNull ColumnPlan") {
    val plan = setNull.bindTo("col")
    assert(plan.isInstanceOf[ColumnPlan.SetNull])
  }

  test("fixed creates Fixed ColumnPlan with correct value") {
    val plan = fixed(42).bindTo("col")
    plan match {
      case ColumnPlan.Fixed(_, v) => assert(v === 42)
      case other                  => fail(s"Expected Fixed, got $other")
    }
  }

  test("using creates Transform ColumnPlan") {
    val plan = using(_.toUpperCase).bindTo("col")
    plan match {
      case ColumnPlan.Transform(_, nav, f) =>
        assert(nav == JsonNav.Direct)
        assert(f("hello") === "HELLO")
      case other                           => fail(s"Expected Transform, got $other")
    }
  }

  test("jsonArray creates Transform ColumnPlan with JSON navigation") {
    val plan = jsonArray("number")(_ => "XXX").bindTo("phones")
    plan match {
      case ColumnPlan.Transform(_, nav, f) =>
        assert(nav.isInstanceOf[JsonNav.ArrayOf])
        assert(f("123") === "XXX")
      case other                           => fail(s"Expected Transform, got $other")
    }
  }

  // ============================================================================
  // DSL setNull and fixed tests
  // ============================================================================

  test("setNull transformer returns null") {
    val transformer = table("field" -> setNull)
    val row         = Map("field" -> "some value")
    val result      = transformer.transform(row)
    assert(result("field") === null)
  }

  test("fixed transformer returns the fixed value") {
    val transformer = table("status" -> fixed("REDACTED"))
    val row         = Map("status" -> "original")
    val result      = transformer.transform(row)
    assert(result("status") === "REDACTED")
  }

  test("fixed with numeric value") {
    val transformer = table("count" -> fixed(0))
    val row         = Map("count" -> "999")
    val result      = transformer.transform(row)
    assert(result("count") === "0") // transform returns String
  }
}
