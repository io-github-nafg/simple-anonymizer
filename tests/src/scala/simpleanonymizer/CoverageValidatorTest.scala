package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funspec.AnyFunSpec

class CoverageValidatorTest extends AnyFunSpec with TypeCheckedTripleEquals {
  import CoverageValidator.{generateTableSnippet, generateColumnSnippets}

  describe("generateTableSnippet") {
    it("creates valid DSL code") {
      val snippet  = generateTableSnippet("users", Seq("first_name", "email"))
      val expected = """"users" -> TableSpec.select { row =>
    Seq(
      row.first_name,
      row.email
    )
  }"""
      assert(snippet === expected)
    }

    it("handles empty columns") {
      val snippet  = generateTableSnippet("empty_table", Seq.empty)
      val expected = "\"empty_table\" -> TableSpec.select { row =>\n    Seq(\n\n    )\n  }"
      assert(snippet === expected)
    }
  }

  describe("generateColumnSnippets") {
    it("creates sorted passthrough entries") {
      val snippet  = generateColumnSnippets(Set("zip", "city", "address"))
      val expected = """row.address,
      row.city,
      row.zip"""
      assert(snippet === expected)
    }

    it("handles empty set") {
      val snippet = generateColumnSnippets(Set.empty)
      assert(snippet === "")
    }

    it("handles single column") {
      val snippet = generateColumnSnippets(Set("name"))
      assert(snippet === "row.name")
    }
  }
}
