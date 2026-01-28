# simple-anonymizer

A Scala library for deterministic data anonymization with a composable DSL.

## Features

- **Deterministic anonymization** — Same input always produces same output (using MD5 hash-based selection)
- **Realistic fake data** — Uses DataFaker's curated data lists for names, addresses, etc.
- **Composable DSL** — Build table transformers by combining column specifications
- **JSON support** — Transform fields within JSON arrays and objects
- **Dependent columns** — Columns that depend on other columns' values

## Installation

Add to your `bleep.yaml`:

```yaml
dependencies:
  - io.github.nafg::simple-anonymizer:VERSION
```

Or for sbt:

```scala
libraryDependencies += "io.github.nafg" %% "simple-anonymizer" % "VERSION"
```

## Quick Start

```scala
import simpleanonymizer.DeterministicAnonymizer.*
import simpleanonymizer.RowTransformer.DSL.*

// Define a table transformer
val personTransformer = table(
  "first_name" -> using(FirstName.anonymize),
  "last_name"  -> using(LastName.anonymize),
  "email"      -> using(Email.anonymize),
  "created_at" -> passthrough
)

// Transform a row
val row = Map(
  "first_name" -> "John",
  "last_name"  -> "Doe",
  "email"      -> "john.doe@company.com",
  "created_at" -> "2024-01-15"
)

val anonymized = personTransformer.transform(row)
// Result: Map(
//   "first_name" -> "Michael",
//   "last_name"  -> "Anderson",
//   "email"      -> "sarah.wilson@example.com",
//   "created_at" -> "2024-01-15"
// )
```

## DSL Reference

### Basic Transformers

| Transformer | Description | Example |
|-------------|-------------|---------|
| `passthrough` | Copy value unchanged | IDs, timestamps, non-PII |
| `using(f)` | Apply transformation function | `using(FirstName.anonymize)` |
| `jsonArray(field)(f)` | Transform field within JSON array | `jsonArray("number")(PhoneNumber.anonymize)` |
| `col(name).map(f)` | Dependent transformation | `col("state").map(st => _ => fakeZipForState(st))` |

### Building Table Transformers

```scala
import simpleanonymizer.RowTransformer.DSL.*

val transformer = table(
  "column1" -> passthrough,
  "column2" -> using(someFunction),
  "column3" -> jsonArray("fieldName")(transformFunc)
)
```

## Available Anonymizers

All anonymizers are in `DeterministicAnonymizer` and implement the `Anonymizer` trait:

```scala
sealed trait Anonymizer {
  def anonymize(input: String): String
}
```

### Name Anonymizers

| Anonymizer | Output Example |
|------------|----------------|
| `FirstName` | "Michael", "Sarah", "David" |
| `MaleFirstName` | "James", "Robert", "William" |
| `FemaleFirstName` | "Mary", "Jennifer", "Linda" |
| `LastName` | "Smith", "Johnson", "Williams" |
| `FullName` | "Michael Anderson", "Sarah Wilson" |

### Contact Anonymizers

| Anonymizer | Output Example |
|------------|----------------|
| `Email` | "michael.anderson@example.com" |
| `PhoneNumber` | "(503) 615-0345" |

### Address Anonymizers

| Anonymizer | Output Example |
|------------|----------------|
| `StreetAddress` | "1234 Wilson Street" |
| `City` | "Andersonburg" |
| `State` | "California" |
| `StateAbbr` | "CA" |
| `ZipCode` | "90210" |
| `Country` | "United States" |

### Other Anonymizers

| Anonymizer | Description | Output Example |
|------------|-------------|----------------|
| `Passthrough` | No change | (original value) |
| `Null` | Replace with null | `null` |
| `Fixed(value)` | Constant replacement | (specified value) |
| `Redact` | Asterisks (preserves length) | "****" |
| `PartialRedact(first, last)` | Partial masking | "Jo**oe" |
| `LoremText` | Lorem ipsum of similar length | "lorem ipsum dolor..." |

## JSON Column Support

Transform specific fields within JSON arrays:

```scala
// Input column value:
// [{"type": "Home", "number": "555-123-4567"}, {"type": "Work", "number": "555-987-6543"}]

val transformer = table(
  "phones" -> jsonArray("number")(PhoneNumber.anonymize)
)

// Output:
// [{"type": "Home", "number": "(503) 615-0345"}, {"type": "Work", "number": "(721) 843-9012"}]
```

## Dependent Column Transformations

When a column's transformation depends on another column's value:

```scala
// Single dependency
val transformer = table(
  "state" -> using(StateAbbr.anonymize),
  "zip"   -> col("state").map { stateValue =>
    originalZip => generateZipForState(stateValue)
  }
)

// Multiple dependencies
val transformer = table(
  "gender"     -> passthrough,
  "first_name" -> col("gender").map {
    case "M" => MaleFirstName.anonymize
    case "F" => FemaleFirstName.anonymize
    case _   => FirstName.anonymize
  }
)

// Two column dependencies
val transformer = table(
  "state" -> using(StateAbbr.anonymize),
  "city"  -> using(City.anonymize),
  "full_address" -> col("state").and(col("city")).map { (state, city) =>
    addr => s"$city, $state"
  }
)
```

## Deterministic Guarantees

All anonymizers use MD5 hashing to ensure:

1. **Consistency** — Same input always produces the same output
2. **Realistic output** — Values come from DataFaker's curated lists
3. **One-way transformation** — Original cannot be recovered from output
4. **Referential integrity** — Same value in different tables produces same anonymized value

This is particularly important when anonymizing databases where the same email or name might appear in multiple tables and must remain consistent.

## Validation

`TableTransformer` provides validation to ensure all columns are covered:

```scala
val transformer = table(
  "first" -> using(FirstName.anonymize),
  "last"  -> using(LastName.anonymize)
)

val expectedColumns = Set("first", "last", "email")

transformer.validateCovers(expectedColumns) match {
  case Left(missing) => println(s"Missing columns: $missing") // Set("email")
  case Right(())     => println("All columns covered")
}
```

## Architecture

```
simpleanonymizer/
├── DeterministicAnonymizer.scala  — Anonymization functions using DataFaker
│   ├── Sealed Anonymizer trait
│   ├── Hash-based deterministic selection
│   └── Pre-fetched data lists for performance
│
└── RowTransformer.scala           — Composable transformer DSL
    ├── ValueTransformer           — Leaf-level string transformations
    ├── JsonNav                    — JSON navigation (Direct, Field, ArrayOf)
    ├── ColumnSpec                 — Column specification with dependencies
    ├── TableTransformer           — Composes column specs
    └── DSL                        — User-facing API
```

## License

Apache 2.0
