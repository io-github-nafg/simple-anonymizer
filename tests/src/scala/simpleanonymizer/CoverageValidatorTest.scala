package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funsuite.AnyFunSuite

class CoverageValidatorTest extends AnyFunSuite with TypeCheckedTripleEquals {
  import CoverageValidator.{generateTableSnippet, generateColumnSnippets}

  // ============================================================================
  // Code Snippet Generation
  // ============================================================================

  test("generateTableSnippet creates valid DSL code") {
    val snippet  = generateTableSnippet("users", Seq("first_name", "email"))
    val expected = """"users" -> TableSpec.select { row =>
    Seq(
      row.first_name,
      row.email
    )
  }"""
    assert(snippet === expected)
  }

  test("generateTableSnippet handles empty columns") {
    val snippet  = generateTableSnippet("empty_table", Seq.empty)
    // When no columns, just generates an empty select body
    val expected = "\"empty_table\" -> TableSpec.select { row =>\n    Seq(\n\n    )\n  }"
    assert(snippet === expected)
  }

  test("generateColumnSnippets creates sorted passthrough entries") {
    val snippet  = generateColumnSnippets(Set("zip", "city", "address"))
    val expected = """row.address,
      row.city,
      row.zip"""
    assert(snippet === expected)
  }

  test("generateColumnSnippets handles empty set") {
    val snippet = generateColumnSnippets(Set.empty)
    assert(snippet === "")
  }

  test("generateColumnSnippets handles single column") {
    val snippet = generateColumnSnippets(Set("name"))
    assert(snippet === "row.name")
  }
}
