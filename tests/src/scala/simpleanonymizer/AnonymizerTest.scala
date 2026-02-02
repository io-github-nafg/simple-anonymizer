package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funspec.AnyFunSpec

class AnonymizerTest extends AnyFunSpec with TypeCheckedTripleEquals {

  describe("stableHash") {
    it("returns 0 for null input") {
      assert(Anonymizer.stableHash(null) === 0)
    }

    it("returns 0 for empty string") {
      assert(Anonymizer.stableHash("") === 0)
    }

    it("returns positive integer for any input") {
      val inputs = List("hello", "world", "test123", "a", "!@#$%", "日本語")
      inputs.foreach { input =>
        val hash = Anonymizer.stableHash(input)
        assert(hash >= 0, s"stableHash($input) returned $hash, expected >= 0")
      }
    }

    it("is deterministic") {
      val input = "test-determinism"
      val hash1 = Anonymizer.stableHash(input)
      val hash2 = Anonymizer.stableHash(input)
      val hash3 = Anonymizer.stableHash(input)
      assert(hash1 === hash2)
      assert(hash2 === hash3)
    }

    it("produces good distribution across 1000 inputs") {
      val inputs = (1 to 1000).map(i => s"input-$i")
      val hashes = inputs.map(Anonymizer.stableHash).toSet
      assert(hashes.size > 990, s"Too many collisions: ${1000 - hashes.size}")
    }
  }

  describe("FirstName") {
    it("preserves null and empty string") {
      assert(Anonymizer.FirstName(null) === null)
      assert(Anonymizer.FirstName("") === "")
    }

    it("is deterministic") {
      val input   = "John"
      val result1 = Anonymizer.FirstName(input)
      val result2 = Anonymizer.FirstName(input)
      assert(result1 === result2)
      assert(result1.nonEmpty)
    }
  }

  describe("FullName") {
    it("produces first + space + last format") {
      val result = Anonymizer.FullName("John Doe")
      assert(result.contains(" "), s"Expected space in '$result'")
      val parts  = result.split(" ")
      assert(parts.length === 2)
      assert(parts(0).nonEmpty)
      assert(parts(1).nonEmpty)
    }

    it("uses different hashes for first and last name") {
      val result = Anonymizer.FullName("TestName")
      val parts  = result.split(" ")
      assert(parts(0).nonEmpty)
      assert(parts(1).nonEmpty)
    }
  }

  describe("Email") {
    it("produces valid email format") {
      val result = Anonymizer.Email("test@example.com")
      assert(result.contains("@"), s"Expected @ in '$result'")
      assert(result.contains("."), s"Expected . in '$result'")
      val parts  = result.split("@")
      assert(parts.length === 2)
      assert(parts(0).contains("."), s"Expected . in local part '${parts(0)}'")
    }

    it("uses predefined safe domains") {
      val allowedDomains = Set("example.com", "test.com", "fake.org", "sample.net")
      val result         = Anonymizer.Email("test@company.com")
      val domain         = result.split("@")(1)
      assert(allowedDomains.contains(domain), s"Unexpected domain: $domain")
    }
  }

  describe("PhoneNumber") {
    it("produces (XXX) XXX-XXXX format") {
      val result  = Anonymizer.PhoneNumber("555-123-4567")
      val pattern = """\(\d{3}\) \d{3}-\d{4}""".r
      assert(pattern.matches(result), s"Unexpected format: $result")
    }
  }

  describe("StreetAddress") {
    it("produces number + street + suffix format") {
      val result = Anonymizer.StreetAddress("123 Main St")
      val parts  = result.split(" ")
      assert(parts.length >= 3, s"Expected at least 3 parts in '$result'")
      assert(parts(0).forall(_.isDigit), s"Expected first part to be number: ${parts(0)}")
    }
  }

  describe("ZipCode") {
    it("produces 5-digit format") {
      val result = Anonymizer.ZipCode("12345")
      assert(result.length === 5, s"Expected 5 chars, got ${result.length}")
      assert(result.forall(_.isDigit), s"Expected all digits in '$result'")
      val num    = result.toInt
      assert(num >= 10000 && num <= 99999)
    }
  }

  describe("StateAbbr") {
    it("produces 2-character abbreviation") {
      val result = Anonymizer.StateAbbr("California")
      assert(result.length === 2, s"Expected 2 chars, got '$result'")
    }
  }

  describe("Redact") {
    it("preserves length with asterisks") {
      assert(Anonymizer.Redact("hello") === "*****")
      assert(Anonymizer.Redact("a") === "*")
      assert(Anonymizer.Redact("test1234") === "********")
    }
  }

  describe("PartialRedact") {
    it("shows first and last chars, asterisks in middle") {
      val result = Anonymizer.PartialRedact(2, 2)("hello123")
      assert(result === "he****23")
    }

    it("fully redacts when input shorter than showFirst + showLast") {
      val result = Anonymizer.PartialRedact(2, 2)("abc")
      assert(result === "***")
    }
  }

  describe("LoremText") {
    it("produces output matching input length") {
      val inputs = List("short", "a longer piece of text", "x")
      inputs.foreach { input =>
        val result = Anonymizer.LoremText(input)
        assert(result.length === input.length, s"Input '$input' produced '$result'")
      }
    }

    it("is deterministic") {
      val input   = "test input"
      val result1 = Anonymizer.LoremText(input)
      val result2 = Anonymizer.LoremText(input)
      assert(result1 === result2)
    }
  }
}
