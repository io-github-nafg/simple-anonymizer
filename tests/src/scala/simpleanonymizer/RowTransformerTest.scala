package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funsuite.AnyFunSuite

class RowTransformerTest extends AnyFunSuite with TypeCheckedTripleEquals {
  import RowTransformer._
  import RowTransformer.DSL._

  // ============================================================================
  // ValueTransformer tests
  // ============================================================================

  test("PassThrough ValueTransformer returns input unchanged") {
    val vt = ValueTransformer.PassThrough
    assert(vt("hello") === "hello")
    assert(vt("") === "")
  }

  test("Simple ValueTransformer applies function") {
    val vt = ValueTransformer.Simple(_.toUpperCase)
    assert(vt("hello") === "HELLO")
  }

  // ============================================================================
  // JsonNav tests
  // ============================================================================

  test("JsonNav.Field transforms a specific field in JSON object") {
    val nav     = JsonNav.Field("name")
    val vt      = ValueTransformer.Simple(_.toUpperCase)
    val wrapped = nav.wrap(vt)
    val result  = wrapped("""{"name":"john","age":30}""")
    assert(result === """{"name":"JOHN","age":30}""")
  }

  test("JsonNav.Field leaves other fields unchanged") {
    val nav     = JsonNav.Field("name")
    val vt      = ValueTransformer.Simple(_ => "REPLACED")
    val wrapped = nav.wrap(vt)
    val result  = wrapped("""{"name":"john","city":"NYC"}""")
    assert(result === """{"name":"REPLACED","city":"NYC"}""")
  }

  test("JsonNav.ArrayOf transforms each element in array") {
    val nav     = JsonNav.ArrayOf(JsonNav.Direct)
    val vt      = ValueTransformer.Simple(_.toUpperCase)
    val wrapped = nav.wrap(vt)
    // ArrayOf with Direct expects array of strings
    val result  = wrapped("""["a","b","c"]""")
    assert(result === """["A","B","C"]""")
  }

  test("JsonNav.ArrayOf with Field transforms nested field in each element") {
    val nav     = JsonNav.ArrayOf(JsonNav.Field("number"))
    val vt      = ValueTransformer.Simple(_ => "XXX")
    val wrapped = nav.wrap(vt)
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
    // (though could collide by chance)
    assert(maleResult("first_name").nonEmpty)
    assert(femaleResult("first_name").nonEmpty)
  }
}
