package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funspec.AnyFunSpec

class LensTest extends AnyFunSpec with TypeCheckedTripleEquals {

  describe("Lens.Direct") {
    it("returns the function unchanged from modify") {
      val f      = (s: String) => s.toUpperCase
      val modify = Lens.Direct.modify(f)
      assert(modify eq f)
    }
  }

  describe("Lens.Field") {
    it("transforms a specific field in a JSON object") {
      val lens   = Lens.Field("name")
      val modify = lens.modify(_.toUpperCase)
      val result = modify("""{"name":"john","age":30}""")
      assert(result === """{"name":"JOHN","age":30}""")
    }

    it("leaves other fields unchanged") {
      val lens   = Lens.Field("name")
      val modify = lens.modify(_ => "REPLACED")
      val result = modify("""{"name":"john","city":"NYC"}""")
      assert(result === """{"name":"REPLACED","city":"NYC"}""")
    }

    it("supports nested navigation via inner lens") {
      val lens   = Lens.Field("address", Lens.Field("city"))
      val modify = lens.modify(_.toUpperCase)
      val result = modify("""{"name":"john","address":{"city":"nyc","zip":"10001"}}""")
      assert(result === """{"name":"john","address":{"city":"NYC","zip":"10001"}}""")
    }

    it("returns original string when parsing fails") {
      val lens   = Lens.Field("name")
      val modify = lens.modify(_.toUpperCase)
      val result = modify("not valid json")
      assert(result === "not valid json")
    }
  }

  describe("Lens.ArrayElements") {
    it("transforms each element in an array") {
      val lens   = Lens.ArrayElements(Lens.Direct)
      val modify = lens.modify(_.toUpperCase)
      val result = modify("""["a","b","c"]""")
      assert(result === """["A","B","C"]""")
    }

    it("transforms a nested field in each element") {
      val lens   = Lens.ArrayElements(Lens.Field("number"))
      val modify = lens.modify(_ => "XXX")
      val input  = """[{"type":"home","number":"123"},{"type":"work","number":"456"}]"""
      val result = modify(input)
      assert(result === """[{"type":"home","number":"XXX"},{"type":"work","number":"XXX"}]""")
    }
  }
}
