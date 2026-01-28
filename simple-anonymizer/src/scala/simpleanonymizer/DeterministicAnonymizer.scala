package simpleanonymizer

import java.security.MessageDigest
import java.util.Locale

import net.datafaker.Faker
import net.datafaker.service.FakerContext

/** Deterministic anonymization using DataFaker's underlying data lists.
  *
  * Instead of generating random fake data, we use a hash of the original value to deterministically select from
  * DataFaker's pre-defined lists. This ensures:
  *   1. Same input always produces the same output (deterministic)
  *   2. Output looks realistic (uses DataFaker's curated data)
  *   3. Original data cannot be recovered from output (one-way hash)
  */
object DeterministicAnonymizer {

  private val locale  = Locale.US
  private val faker   = new Faker(locale)
  private val context = new FakerContext(locale, faker.random())
  private val service = faker.fakeValuesService()

  /** Compute a stable hash code for a string, returning a positive integer. Uses MD5 for better distribution than
    * hashCode().
    */
  private[simpleanonymizer] def stableHash(input: String): Int = {
    if (input == null || input.isEmpty) return 0
    val md    = MessageDigest.getInstance("MD5")
    val bytes = md.digest(input.getBytes("UTF-8"))
    // Use the first 4 bytes as an int, make positive
    val hash  = ((bytes(0) & 0xff) << 24) |
      ((bytes(1) & 0xff) << 16) |
      ((bytes(2) & 0xff) << 8) |
      (bytes(3) & 0xff)
    hash & Int.MaxValue // Ensure positive
  }

  /** Select an element from a list deterministically based on the hash of input. */
  private def selectByHash[A](input: String, list: java.util.List[A]): A = {
    val size  = list.size()
    val index = stableHash(input) % size
    list.get(index)
  }

  /** Get a raw data list from DataFaker's YAML files. */
  private def getDataList(key: String): java.util.List[String] =
    service.fetchObject[java.util.List[String]](key, context)

  // Pre-fetch commonly used data lists for performance
  // Note: name.first_name contains template expressions, we need the actual name lists
  private lazy val maleFirstNames   = getDataList("name.male_first_name")
  private lazy val femaleFirstNames = getDataList("name.female_first_name")
  // Combine male and female names for a gender-neutral first name list
  private lazy val firstNames       = {
    val combined = new java.util.ArrayList[String](maleFirstNames.size() + femaleFirstNames.size())
    combined.addAll(maleFirstNames)
    combined.addAll(femaleFirstNames)
    combined
  }
  private lazy val lastNames        = getDataList("name.last_name")
  private lazy val streetSuffixes   = getDataList("address.street_suffix")
  private lazy val citySuffixes     = getDataList("address.city_suffix")
  private lazy val states           = getDataList("address.state")
  private lazy val stateAbbrs       = getDataList("address.state_abbr")
  private lazy val countries        = getDataList("address.country")

  /** Sealed trait for different anonymization strategies */
  sealed trait Anonymizer {
    def anonymize(input: String): String

    /** Helper to preserve null/empty inputs, applying transform only to non-empty values */
    protected final def preserveNullOrEmpty(input: String)(transform: String => String): String =
      if (input == null || input.isEmpty) input else transform(input)
  }

  /** Pass through without modification */
  case object Passthrough extends Anonymizer {
    def anonymize(input: String): String = input
  }

  /** Replace with NULL */
  case object Null extends Anonymizer {
    def anonymize(input: String): String = null
  }

  /** Replace with a fixed value */
  case class Fixed(value: String) extends Anonymizer {
    def anonymize(input: String): String = value
  }

  /** Deterministic first name */
  case object FirstName extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input)(selectByHash(_, firstNames))
  }

  /** Deterministic male first name */
  case object MaleFirstName extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input)(selectByHash(_, maleFirstNames))
  }

  /** Deterministic female first name */
  case object FemaleFirstName extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input)(selectByHash(_, femaleFirstNames))
  }

  /** Deterministic last name */
  case object LastName extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input)(selectByHash(_, lastNames))
  }

  /** Deterministic full name (first + last) */
  case object FullName extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input) { in =>
        val first = selectByHash(in, firstNames)
        // Use a different hash for last name to avoid correlation
        val last  = selectByHash(in + "_last", lastNames)
        s"$first $last"
      }
  }

  /** Deterministic email based on input */
  case object Email extends Anonymizer {
    private val domains = List("example.com", "test.com", "fake.org", "sample.net")

    def anonymize(input: String): String =
      preserveNullOrEmpty(input) { in =>
        val first  = selectByHash(in, firstNames).toLowerCase
        val last   = selectByHash(in + "_last", lastNames).toLowerCase
        val domain = domains(stableHash(in + "_domain") % domains.size)
        s"$first.$last@$domain"
      }
  }

  /** Deterministic phone number - preserves format but changes digits */
  case object PhoneNumber extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input) { in =>
        val hash   = stableHash(in)
        // Generate a consistent 10-digit number
        val digits = (0 until 10).map(i => ((hash >> (i % 30)) & 0xf) % 10)
        // Format as (XXX) XXX-XXXX
        s"(${digits.take(3).mkString}) ${digits.slice(3, 6).mkString}-${digits.slice(6, 10).mkString}"
      }
  }

  /** Deterministic street address */
  case object StreetAddress extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input) { in =>
        val hash   = stableHash(in)
        val number = (hash % 9999) + 1
        val street = selectByHash(in + "_street", lastNames)
        val suffix = selectByHash(in + "_suffix", streetSuffixes)
        s"$number $street $suffix"
      }
  }

  /** Deterministic city name */
  case object City extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input) { in =>
        val name   = selectByHash(in, lastNames)
        val suffix = selectByHash(in + "_suffix", citySuffixes)
        s"$name$suffix"
      }
  }

  /** Deterministic state */
  case object State extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input)(selectByHash(_, states))
  }

  /** Deterministic state abbreviation */
  case object StateAbbr extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input)(selectByHash(_, stateAbbrs))
  }

  /** Deterministic zip code based on state abbreviation */
  case object ZipCode extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input) { in =>
        val hash = stableHash(in)
        // Generate 5-digit zip code
        f"${(hash % 90000) + 10000}%05d"
      }
  }

  /** Deterministic country */
  case object Country extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input)(selectByHash(_, countries))
  }

  /** Redact with asterisks, preserving length */
  case object Redact extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input)(in => "*" * in.length)
  }

  /** Partial redact - show first N and last M characters */
  case class PartialRedact(showFirst: Int = 2, showLast: Int = 2) extends Anonymizer {
    def anonymize(input: String): String =
      preserveNullOrEmpty(input) { in =>
        if (in.length <= showFirst + showLast) "*" * in.length
        else {
          val first  = in.take(showFirst)
          val last   = in.takeRight(showLast)
          val middle = "*" * (in.length - showFirst - showLast)
          s"$first$middle$last"
        }
      }
  }

  /** Lorem ipsum text of similar length */
  case object LoremText extends Anonymizer {
    private val loremWords = List(
      "lorem",
      "ipsum",
      "dolor",
      "sit",
      "amet",
      "consectetur",
      "adipiscing",
      "elit",
      "sed",
      "do",
      "eiusmod",
      "tempor",
      "incididunt",
      "ut",
      "labore",
      "et",
      "dolore",
      "magna",
      "aliqua"
    )

    def anonymize(input: String): String =
      preserveNullOrEmpty(input) { in =>
        val targetLength = in.length
        val hash         = stableHash(in)
        val result       = new StringBuilder()
        var wordIndex    = hash

        while (result.length < targetLength) {
          if (result.nonEmpty) result.append(" ")
          val word = loremWords(wordIndex % loremWords.size)
          result.append(word)
          wordIndex += 1
        }

        result.toString.take(targetLength)
      }
  }
}
