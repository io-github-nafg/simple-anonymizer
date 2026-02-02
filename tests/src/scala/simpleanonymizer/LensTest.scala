package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funsuite.AnyFunSuite

class LensTest extends AnyFunSuite with TypeCheckedTripleEquals {

  // ============================================================================
  // Lens tests
  // ============================================================================

  test("Lens.Field transforms a specific field in JSON object") {
    val lens   = Lens.Field("name")
    val modify = lens.modify(_.toUpperCase)
    val result = modify("""{"name":"john","age":30}""")
    assert(result === """{"name":"JOHN","age":30}""")
  }

  test("Lens.Field leaves other fields unchanged") {
    val lens   = Lens.Field("name")
    val modify = lens.modify(_ => "REPLACED")
    val result = modify("""{"name":"john","city":"NYC"}""")
    assert(result === """{"name":"REPLACED","city":"NYC"}""")
  }

  test("Lens.ArrayElements transforms each element in array") {
    val lens   = Lens.ArrayElements(Lens.Direct)
    val modify = lens.modify(_.toUpperCase)
    val result = modify("""["a","b","c"]""")
    assert(result === """["A","B","C"]""")
  }

  test("Lens.ArrayElements with Field transforms nested field in each element") {
    val lens   = Lens.ArrayElements(Lens.Field("number"))
    val modify = lens.modify(_ => "XXX")
    val input  = """[{"type":"home","number":"123"},{"type":"work","number":"456"}]"""
    val result = modify(input)
    assert(result === """[{"type":"home","number":"XXX"},{"type":"work","number":"XXX"}]""")
  }

  test("Lens.Direct.modify returns the function unchanged") {
    val f      = (s: String) => s.toUpperCase
    val modify = Lens.Direct.modify(f)
    assert(modify eq f)
  }

  test("Lens.Field with inner lens for nested navigation") {
    val lens   = Lens.Field("address", Lens.Field("city"))
    val modify = lens.modify(_.toUpperCase)
    val result = modify("""{"name":"john","address":{"city":"nyc","zip":"10001"}}""")
    assert(result === """{"name":"john","address":{"city":"NYC","zip":"10001"}}""")
  }

  test("JsonLens returns original string when parsing fails") {
    val lens   = Lens.Field("name")
    val modify = lens.modify(_.toUpperCase)
    val result = modify("not valid json")
    assert(result === "not valid json")
  }
}
