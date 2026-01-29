package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funsuite.AnyFunSuite

class DeterministicAnonymizerTest extends AnyFunSuite with TypeCheckedTripleEquals {
  import DeterministicAnonymizer._

  // ============================================================================
  // stableHash - core determinism guarantees
  // ============================================================================

  test("stableHash returns 0 for null input") {
    assert(stableHash(null) === 0)
  }

  test("stableHash returns 0 for empty string") {
    assert(stableHash("") === 0)
  }

  test("stableHash returns positive integer for any input") {
    val inputs = List("hello", "world", "test123", "a", "!@#$%", "日本語")
    inputs.foreach { input =>
      val hash = stableHash(input)
      assert(hash >= 0, s"stableHash($input) returned $hash, expected >= 0")
    }
  }

  test("stableHash is deterministic - same input produces same output") {
    val input = "test-determinism"
    val hash1 = stableHash(input)
    val hash2 = stableHash(input)
    val hash3 = stableHash(input)
    assert(hash1 === hash2)
    assert(hash2 === hash3)
  }

  test("stableHash produces good distribution - different inputs rarely collide") {
    val inputs = (1 to 1000).map(i => s"input-$i")
    val hashes = inputs.map(stableHash).toSet
    // With 1000 inputs, we should have very few collisions
    assert(hashes.size > 990, s"Too many collisions: ${1000 - hashes.size}")
  }

  // ============================================================================
  // Name anonymizers - determinism and null/empty handling
  // ============================================================================

  test("FirstName preserves null and empty string") {
    assert(FirstName.anonymize(null) === null)
    assert(FirstName.anonymize("") === "")
  }

  test("FirstName is deterministic") {
    val input   = "John"
    val result1 = FirstName.anonymize(input)
    val result2 = FirstName.anonymize(input)
    assert(result1 === result2)
    assert(result1.nonEmpty)
  }

  test("FullName produces first + space + last format") {
    val result = FullName.anonymize("John Doe")
    assert(result.contains(" "), s"Expected space in '$result'")
    val parts  = result.split(" ")
    assert(parts.length === 2)
    assert(parts(0).nonEmpty)
    assert(parts(1).nonEmpty)
  }

  test("FullName uses different hashes for first and last name") {
    val result = FullName.anonymize("TestName")
    val parts  = result.split(" ")
    assert(parts(0).nonEmpty)
    assert(parts(1).nonEmpty)
  }

  // ============================================================================
  // Contact anonymizers - format validation
  // ============================================================================

  test("Email produces valid email format") {
    val result = Email.anonymize("test@example.com")
    assert(result.contains("@"), s"Expected @ in '$result'")
    assert(result.contains("."), s"Expected . in '$result'")
    val parts  = result.split("@")
    assert(parts.length === 2)
    assert(parts(0).contains("."), s"Expected . in local part '${parts(0)}'")
  }

  test("Email uses predefined safe domains") {
    val allowedDomains = Set("example.com", "test.com", "fake.org", "sample.net")
    val result         = Email.anonymize("test@company.com")
    val domain         = result.split("@")(1)
    assert(allowedDomains.contains(domain), s"Unexpected domain: $domain")
  }

  test("PhoneNumber produces (XXX) XXX-XXXX format") {
    val result  = PhoneNumber.anonymize("555-123-4567")
    val pattern = """\(\d{3}\) \d{3}-\d{4}""".r
    assert(pattern.matches(result), s"Unexpected format: $result")
  }

  // ============================================================================
  // Address anonymizers - format validation
  // ============================================================================

  test("StreetAddress produces number + street + suffix format") {
    val result = StreetAddress.anonymize("123 Main St")
    val parts  = result.split(" ")
    assert(parts.length >= 3, s"Expected at least 3 parts in '$result'")
    assert(parts(0).forall(_.isDigit), s"Expected first part to be number: ${parts(0)}")
  }

  test("ZipCode produces 5-digit format") {
    val result = ZipCode.anonymize("12345")
    assert(result.length === 5, s"Expected 5 chars, got ${result.length}")
    assert(result.forall(_.isDigit), s"Expected all digits in '$result'")
    val num    = result.toInt
    assert(num >= 10000 && num <= 99999)
  }

  test("StateAbbr produces 2-character abbreviation") {
    val result = StateAbbr.anonymize("California")
    assert(result.length === 2, s"Expected 2 chars, got '$result'")
  }

  // ============================================================================
  // Redaction anonymizers - length preservation
  // ============================================================================

  test("Redact preserves length with asterisks") {
    assert(Redact.anonymize("hello") === "*****")
    assert(Redact.anonymize("a") === "*")
    assert(Redact.anonymize("test1234") === "********")
  }

  test("PartialRedact shows first and last chars, asterisks in middle") {
    val result = PartialRedact(2, 2).anonymize("hello123")
    assert(result === "he****23")
  }

  test("PartialRedact fully redacts when input shorter than showFirst + showLast") {
    val result = PartialRedact(2, 2).anonymize("abc")
    assert(result === "***")
  }

  // ============================================================================
  // LoremText - length matching
  // ============================================================================

  test("LoremText produces output matching input length") {
    val inputs = List("short", "a longer piece of text", "x")
    inputs.foreach { input =>
      val result = LoremText.anonymize(input)
      assert(result.length === input.length, s"Input '$input' produced '$result'")
    }
  }

  test("LoremText is deterministic") {
    val input   = "test input"
    val result1 = LoremText.anonymize(input)
    val result2 = LoremText.anonymize(input)
    assert(result1 === result2)
  }
}
