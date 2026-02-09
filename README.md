# simple-anonymizer

A Scala library for creating anonymized copies of PostgreSQL databases for development, staging, or testing environments. Provides a composable DSL for defining per-table transformations with deterministic fake data that preserves referential integrity.

## Features

- **Deterministic anonymization** — Same input always produces same output (using MD5 hash-based selection)
- **Realistic fake data** — Uses DataFaker's curated data lists for names, addresses, etc.
- **Composable DSL** — Build table transformers using `TableSpec.select { row => Seq(...) }` syntax
- **JSON support** — Transform fields within JSON arrays and objects
- **Database copying** — Copy tables between PostgreSQL databases with optional transformations
- **FK-aware ordering** — Automatically determine correct table copy order based on foreign keys
- **Filter propagation** — WHERE clauses on parent tables automatically propagate to child tables via FK subqueries
- **Upsert support** — Handle pre-existing data with `ON CONFLICT DO UPDATE` or `DO NOTHING`
- **Validation** — Fails with copy-pastable code snippets if you miss a table or column

## Installation

Add to your `bleep.yaml`:

```yaml
dependencies:
  - io.github.nafg.simple-anonymizer::simple-anonymizer:VERSION
```

Or for sbt:

```scala
libraryDependencies += "io.github.nafg.simple-anonymizer" %% "simple-anonymizer" % "VERSION"
```

## Quick Start

```scala
import simpleanonymizer.{Anonymizer, TableSpec}

// Define a table spec with transformations.
// PK and FK columns (e.g., id, user_id) are passed through automatically by DbCopier
// and don't need to be listed here — only non-PK/non-FK columns are required.
val personSpec = TableSpec.select { row =>
  Seq(
    row.first_name.mapString(Anonymizer.FirstName),
    row.last_name.mapString(Anonymizer.LastName),
    row.email.mapString(Anonymizer.Email),
    row.created_at  // passthrough
  )
}
```

## Database Copying

Copy entire PostgreSQL databases with automatic FK ordering, filter propagation, and validation. Uses Slick for async database operations.

```scala
import simpleanonymizer.{Anonymizer, DbCopier, TableSpec}
import simpleanonymizer.SlickProfile.api._
import scala.concurrent.ExecutionContext.Implicits.global

val sourceDb = Database.forURL(sourceUrl, user, pass)
val targetDb = Database.forURL(targetUrl, user, pass)
val copier = new DbCopier(sourceDb, targetDb)

for {
  result <- copier.run(
    // Only non-PK/non-FK columns need to be listed.
    // PK (id) and FK (user_id) columns are copied automatically.
    "users"    -> TableSpec.select { row =>
      Seq(
        row.first_name.mapString(Anonymizer.FirstName),
        row.last_name.mapString(Anonymizer.LastName),
        row.email.mapString(Anonymizer.Email)
      )
    }.where("active = true"),  // Optional filter — propagates to child tables
    "orders"   -> TableSpec.select { row =>
      Seq(row.description, row.amount)  // Type preserved (DECIMAL)
    },
    "profiles" -> TableSpec.select { row =>
      Seq(
        row.phones.mapJsonArray(_.number.mapString(Anonymizer.PhoneNumber)),
        row.settings
      )
    }
  )
} yield result  // Map[tableName -> rowCount]
```

`DbCopier` automatically:
- Sorts tables by FK dependencies so parent rows exist before child rows
- Propagates WHERE clauses through FKs (e.g., filtering `users` also filters `orders` referencing those users)
- Passes through PK and FK columns as-is — only non-PK/non-FK columns need to be in the spec (you _can_ include PK/FK columns to override the passthrough, e.g., to transform them)
- Validates that every table and every non-PK/non-FK column is covered, with copy-pastable code snippets on failure
- Defers self-referencing FK constraints automatically (requires PostgreSQL 9.4+)

### Skipping Tables

```scala
val copier = new DbCopier(sourceDb, targetDb, skippedTables = Set("audit_logs", "temp_data"))
```

### Handling Existing Data (ON CONFLICT)

By default, copying into a table that already has rows will fail on primary key conflicts. Use `onConflict` to control this behavior:

```scala
copier.run(
  // Update existing rows with new values on PK conflict (auto-detected)
  "users" -> TableSpec.select { row =>
    Seq(row.first_name.mapString(Anonymizer.FirstName), row.last_name, row.email)
  }.onConflict(OnConflict.doUpdate),

  // Skip conflicting rows without error (auto-detected PK)
  "categories" -> TableSpec.select(row => Seq(row.name))
    .onConflict(OnConflict.doNothing),

  // Explicit conflict target columns (required when using TableCopier directly)
  "orders" -> TableSpec.select(row => Seq(row.status, row.total))
    .onConflict(OnConflict.doUpdate("order_number")),

  // Named constraint
  "items" -> TableSpec.select(row => Seq(row.name, row.quantity))
    .onConflict(OnConflict(OnConflict.ConflictTarget.Constraint("items_unique_name"), OnConflict.Action.DoNothing))
)
```

| Factory Method | Conflict Target | Action |
|----------------|-----------------|--------|
| `OnConflict.doUpdate` | Primary key (auto-detected) | Update all non-PK columns |
| `OnConflict.doNothing` | Primary key (auto-detected) | Skip conflicting rows |
| `OnConflict.doUpdate("col1", "col2")` | Explicit columns | Update all non-conflict columns |
| `OnConflict.doNothing("col1")` | Explicit columns | Skip conflicting rows |

### Validation

`DbCopier.run()` validates that all tables and columns are covered. If you miss something, it fails with copy-pastable code snippets:

```
Missing table specs for 2 table(s).

Add these tables to copier.run(...):

"products" -> TableSpec.select { row =>
    Seq(
      row.name,
      row.description
    )
  }

Or skip them via DbCopier(skippedTables = Set("products"))
```

## DSL Reference

### Column Transformations

| Syntax | Description | Example |
|--------|-------------|---------|
| `row.column` | Passthrough (preserves original type) | IDs, timestamps, numeric columns |
| `row.column.mapString(f)` | Apply String => String transformation | `row.email.mapString(Anonymizer.Email)` |
| `row.column.mapOptString(f)` | Apply Option[String] => Option[String] transformation | Handle null values explicitly |
| `row.column.nulled` | Replace with null | Clear sensitive fields |
| `row.column := value` | Constant replacement (any type) | `row.status := "REDACTED"` |
| `row.column.mapJsonArray(_.field.mapString(f))` | Transform field within JSON array | See below |

## Available Anonymizers

Anonymizers are `String => String` functions that produce realistic fake data. Pass them directly to `mapString`:

### Name Anonymizers

| Anonymizer | Output Example |
|------------|----------------|
| `Anonymizer.FirstName` | "Michael", "Sarah", "David" |
| `Anonymizer.MaleFirstName` | "James", "Robert", "William" |
| `Anonymizer.FemaleFirstName` | "Mary", "Jennifer", "Linda" |
| `Anonymizer.LastName` | "Smith", "Johnson", "Williams" |
| `Anonymizer.FullName` | "Michael Anderson", "Sarah Wilson" |

### Contact Anonymizers

| Anonymizer | Output Example |
|------------|----------------|
| `Anonymizer.Email` | "michael.anderson@example.com" |
| `Anonymizer.PhoneNumber` | "(503) 615-0345" |

### Address Anonymizers

| Anonymizer | Output Example |
|------------|----------------|
| `Anonymizer.StreetAddress` | "1234 Wilson Street" |
| `Anonymizer.City` | "Andersonburg" |
| `Anonymizer.State` | "California" |
| `Anonymizer.StateAbbr` | "CA" |
| `Anonymizer.ZipCode` | "90210" |
| `Anonymizer.Country` | "United States" |

### Other Anonymizers

| Anonymizer | Description | Output Example |
|------------|-------------|----------------|
| `Anonymizer.Redact` | Asterisks (preserves length) | "****" |
| `Anonymizer.PartialRedact(first, last)` | Partial masking | "Jo**oe" |
| `Anonymizer.LoremText` | Lorem ipsum of similar length | "lorem ipsum dolor..." |

## JSON Column Support

Transform specific fields within JSON arrays:

```scala
// Input column value:
// [{"type": "Home", "number": "555-123-4567"}, {"type": "Work", "number": "555-987-6543"}]

val spec = TableSpec.select { row =>
  Seq(row.phones.mapJsonArray(_.number.mapString(Anonymizer.PhoneNumber)))
}

// Output:
// [{"type": "Home", "number": "(503) 615-0345"}, {"type": "Work", "number": "(721) 843-9012"}]
```

## Null Handling

`mapString` preserves null values — if the source column is null, the transformation function is not called and the output remains null. This is the desired behavior for anonymization (there's nothing sensitive about a missing value).

For cases where you need to handle null values explicitly, use `mapOptString`:

```scala
val spec = TableSpec.select { row =>
  Seq(
    row.email.mapOptString {
      case None    => Some("default@example.com")  // Replace null with default
      case Some(e) => Some(Anonymizer.Email(e))     // Anonymize non-null
    }
  )
}
```

## Deterministic Guarantees

All anonymizers use MD5 hashing to ensure:

1. **Consistency** — Same input always produces the same output
2. **Realistic output** — Values come from DataFaker's curated lists
3. **One-way transformation** — Original cannot be recovered from output
4. **Referential integrity** — Same value in different tables produces same anonymized value

This is particularly important when anonymizing databases where the same email or name might appear in multiple tables and must remain consistent.

## License

Apache 2.0
