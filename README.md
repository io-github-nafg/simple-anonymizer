# simple-anonymizer

A Scala library for deterministic data anonymization with a composable DSL, plus database copying with transformations.

## Features

- **Deterministic anonymization** — Same input always produces same output (using MD5 hash-based selection)
- **Realistic fake data** — Uses DataFaker's curated data lists for names, addresses, etc.
- **Composable DSL** — Build table transformers using `TableSpec.select { row => Seq(...) }` syntax
- **JSON support** — Transform fields within JSON arrays and objects
- **Database copying** — Copy tables between PostgreSQL databases with optional transformations
- **FK-aware ordering** — Automatically determine correct table copy order based on foreign keys

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
import simpleanonymizer.{Anonymizer, TableSpec}

// Define a table spec with transformations
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

Copy tables between PostgreSQL databases with optional anonymization. Uses Slick for async database operations:

```scala
import simpleanonymizer.{Anonymizer, TableCopier, TableSpec}
import simpleanonymizer.SlickProfile.api._
import scala.concurrent.ExecutionContext.Implicits.global

val sourceDb = Database.forURL(sourceUrl, user, pass)
val targetDb = Database.forURL(targetUrl, user, pass)

// Define transformer for sensitive columns
val userSpec = TableSpec.select { row =>
  Seq(
    row.first_name.mapString(Anonymizer.FirstName),
    row.last_name.mapString(Anonymizer.LastName),
    row.email.mapString(Anonymizer.Email)
  )
}

// Copy with transformation
for {
  columns <- sourceDb.run(MColumn.getColumns(table, "%")).map(_.map(_.name))
  count   <- TableCopier.copyTable(
               sourceDb = sourceDb,
               targetDb = targetDb,
               schema = "public",
               tableName = "users",
               columns = columns,
               tableSpec = Some(userSpec)
             )
} yield count
```

### High-Level DbCopier API

For copying entire databases with automatic FK ordering and filter propagation.
Requires explicit handling for all tables - either a transformer or explicit skip.

```scala
import simpleanonymizer.{Anonymizer, DbCopier, TableSpec}

val copier = new DbCopier(sourceDb, targetDb)

// Define handling for each table
for {
  result <- copier.run(
    "users"    -> TableSpec.select { row =>
      Seq(
        row.first_name.mapString(Anonymizer.FirstName),
        row.last_name.mapString(Anonymizer.LastName),
        row.email.mapString(Anonymizer.Email)
      )
    }.where("active = true"),  // Optional filter
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

#### TableSpec Options

| Method | Description |
|--------|-------------|
| `TableSpec.select { row => Seq(...) }` | Copy with transformation |
| `select { row => Seq(...) }.where("...")` | Copy with filter |

Use the `skippedTables` constructor parameter to skip tables:

```scala
val copier = new DbCopier(sourceDb, targetDb, skippedTables = Set("audit_logs", "temp_data"))
```

If you miss a table or column, the error message includes copy-pastable code snippets:

```
Missing table specs for 2 table(s).

Add these tables to copier.run(...):

"products" -> TableSpec.select { row =>
    Seq(
      row.name,
      row.description
    )
  }
```

### Copy Options

```scala
TableCopier.copyTable(
  sourceDb = sourceDb,
  targetDb = targetDb,
  schema = "public",
  tableName = "users",
  columns = columns,
  whereClause = Some("created_at > '2024-01-01'"),  // Filter rows
  limit = Some(1000),                                // Limit row count
  tableSpec = Some(userSpec)                         // Apply anonymization
)
```

### FK-Aware Table Ordering

The `TableSorter` module handles topological sorting based on foreign key dependencies:

```scala
import simpleanonymizer.TableSorter

for {
  tables        <- sourceDb.run(dbMetadata.getAllTables)
  fks           <- sourceDb.run(dbMetadata.getAllForeignKeys)
  orderedGroups  = TableSorter(tables, fks)
  // Returns Seq[Seq[MTable]] - tables grouped by level
  // Level 0: no dependencies, Level 1: depends on level 0, etc.
} yield orderedGroups
```

### Filter Propagation

The `FilterPropagation` module automatically propagates WHERE clauses through FK relationships:

```scala
import simpleanonymizer.FilterPropagation

for {
  tables <- sourceDb.run(dbMetadata.getAllTables)
  fks    <- sourceDb.run(dbMetadata.getAllForeignKeys)
} yield {
  val tableSpecs = Map(
    "users" -> TableSpec.select { row => Seq(row.name, row.email) }.where("active = true")
  )
  val effectiveFilters = FilterPropagation.computeEffectiveFilters(tables.map(_.name.name), fks, tableSpecs)
  // Child tables (orders, profiles) automatically get:
  // "user_id IN (SELECT id FROM users WHERE active = true)"
}
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

### Composing Columns

Use `Seq(...)` to compose multiple column transformations:

```scala
import simpleanonymizer.{Anonymizer, TableSpec}

val spec = TableSpec.select { row =>
  Seq(
    row.first_name.mapString(Anonymizer.FirstName),
    row.last_name.mapString(Anonymizer.LastName),
    row.email.mapString(Anonymizer.Email),
    row.created_at  // passthrough
  )
}
```

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

## Validation

`TableSpec` provides validation to ensure all columns are covered:

```scala
val spec = TableSpec.select { row =>
  Seq(
    row.first.mapString(Anonymizer.FirstName),
    row.last.mapString(Anonymizer.LastName)
  )
}

val expectedColumns = Set("first", "last", "email")

spec.validateCovers(expectedColumns) match {
  case Left(missing) => println(s"Missing columns: $missing") // Set("email")
  case Right(())     => println("All columns covered")
}
```

## Architecture

```
simpleanonymizer/
├── Anonymizer.scala               — Anonymization functions using DataFaker
│   ├── Anonymizer trait (extends String => String)
│   ├── Hash-based deterministic selection
│   └── Pre-fetched data lists for performance
│
├── Lens.scala                     — JSON navigation
│   └── Lens                       — Direct, Field, ArrayElements
│
├── OutputColumn.scala             — Column output specifications
│   ├── SourceColumn               — Passthrough (preserves type)
│   ├── TransformedColumn          — String transformation (null-preserving)
│   └── FixedColumn                — Fixed value of any type
│
├── TableSpec.scala                — Table specification & DSL entry point
│   ├── TableSpec.select { row => Seq(...) } — Entry point (returns TableSpec)
│   ├── columns: Seq[OutputColumn] — Column transformations
│   ├── whereClause: Option[String] — Optional filter
│   └── validateCovers             — Column coverage validation
│
├── TableCopier.scala              — Low-level table copy operations
│   └── copyTable                  — Copy with optional transformation
│
├── DbCopier.scala                 — High-level orchestrator (Slick-based)
│   └── run                        — Copy all tables with FK ordering
│
├── CoverageValidator.scala        — Validation and error messages
│   ├── getDataColumns             — List non-PK, non-FK columns
│   ├── ensureAllColumns           — Validate all columns are covered
│   └── ensureAllTables            — Validate all tables are handled
│
├── DbMetadata.scala               — Database schema introspection
│   ├── getAllTables               — List tables in schema
│   └── getAllForeignKeys          — Get all FK relationships
│
├── TableSorter.scala              — FK-based table ordering
│   └── apply(tables, fks)         — Returns tables grouped by dependency level
│
├── FilterPropagation.scala        — WHERE clause propagation
│   └── computeEffectiveFilters    — Propagate filters through FKs
│
└── SlickProfile.scala             — PostgreSQL profile with slick-pg
```

## License

Apache 2.0
