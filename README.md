# simple-anonymizer

A Scala library for deterministic data anonymization with a composable DSL, plus database copying with transformations.

## Features

- **Deterministic anonymization** — Same input always produces same output (using MD5 hash-based selection)
- **Realistic fake data** — Uses DataFaker's curated data lists for names, addresses, etc.
- **Composable DSL** — Build table transformers by combining column specifications
- **JSON support** — Transform fields within JSON arrays and objects
- **Dependent columns** — Columns that depend on other columns' values
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

## Database Copying

Copy tables between PostgreSQL databases with optional anonymization. Uses Slick for async database operations:

```scala
import simpleanonymizer.DbSnapshot.*
import simpleanonymizer.SlickProfile.api.*
import simpleanonymizer.DeterministicAnonymizer.*
import simpleanonymizer.RowTransformer.DSL.*
import scala.concurrent.ExecutionContext.Implicits.global

val sourceDb = Database.forURL(sourceUrl, user, pass)
val targetDb = Database.forURL(targetUrl, user, pass)

// Define transformer for sensitive columns
val userTransformer = table(
  "first_name" -> using(FirstName.anonymize),
  "last_name"  -> using(LastName.anonymize),
  "email"      -> using(Email.anonymize)
)

// Copy with transformation
for {
  columns <- sourceDb.run(getTableColumns("users"))
  count   <- copyTable(
               sourceDb = sourceDb,
               targetDb = targetDb,
               tableName = "users",
               columns = columns,
               transformer = Some(userTransformer)
             )
} yield count
```

### High-Level Snapshot API

For copying entire databases with automatic FK ordering and filter propagation.
Requires explicit handling for all tables - either a transformer or explicit skip.

```scala
import simpleanonymizer.{Snapshot, DbSnapshot}
import simpleanonymizer.DbSnapshot.TableSpec
import simpleanonymizer.DbSnapshot.TableSpec.*
import simpleanonymizer.RowTransformer.DSL.*
import simpleanonymizer.DeterministicAnonymizer.*

val snapshot = new Snapshot(sourceDb, targetDb)

// Define handling for each table using TableSpec
for {
  result <- snapshot.copy(Map(
    "users"      -> copy(
      table(
        "first_name" -> using(FirstName.anonymize),
        "last_name"  -> using(LastName.anonymize),
        "email"      -> using(Email.anonymize)
      ),
      whereClause = "active = true"  // Optional filter
    ),
    "orders"     -> copy(table(
      "description" -> passthrough,
      "amount"      -> passthrough  // Type preserved (DECIMAL)
    )),
    "audit_logs" -> skip  // Exclude from copy
  ))
} yield result  // Map[tableName -> rowCount]
```

#### TableSpec Options

| Method | Description |
|--------|-------------|
| `skip` | Skip table entirely |
| `copy(transformer)` | Copy with transformer |
| `copy(transformer, whereClause)` | Copy with filter |
| `copyAll(transformer)` | Copy all rows (ignore FK filter propagation) |

If you miss a table or column, the error message includes copy-pastable code snippets:

```
Missing transformers for 2 table(s).

Add these tables to 'transformers':

"products" -> table(
    "name" -> passthrough,
    "description" -> passthrough
  ),

Or mark them as skipped in 'tableConfigs':
"products" -> TableConfig(skip = true),
```

### Copy Options

```scala
copyTable(
  sourceDb = sourceDb,
  targetDb = targetDb,
  tableName = "users",
  columns = columns,
  whereClause = Some("created_at > '2024-01-01'"),  // Filter rows
  limit = Some(1000),                                // Limit row count
  transformer = Some(userTransformer)                // Apply anonymization
)
```

### FK-Aware Table Ordering

Copy tables in the correct order based on foreign key dependencies:

```scala
for {
  tables        <- sourceDb.run(getAllTables())
  fks           <- sourceDb.run(getForeignKeys())
  tableLevels    = computeTableLevels(tables, fks)
  orderedGroups  = groupTablesByLevel(tableLevels)
  // Copy in order: level 0 (no dependencies), then level 1, etc.
  _ <- orderedGroups.foldLeft(Future.successful(())) { (prev, group) =>
    prev.flatMap { _ =>
      Future.traverse(group) { table =>
        for {
          cols  <- sourceDb.run(getTableColumns(table))
          count <- copyTable(sourceDb, targetDb, table, cols)
        } yield count
      }
    }
  }
} yield ()
```

### Filter Propagation

Automatically propagate WHERE clauses through FK relationships:

```scala
for {
  tables <- sourceDb.run(getAllTables())
  fks    <- sourceDb.run(getForeignKeys())
} yield {
  val tableConfigs = Map(
    "users" -> TableConfig(whereClause = Some("active = true"))
  )
  val effectiveFilters = computeEffectiveFilters(tables, fks, tableConfigs)
  // Child tables (orders, profiles) automatically get:
  // "user_id IN (SELECT id FROM users WHERE active = true)"
}
```

## DSL Reference

### Basic Transformers

| Transformer | Description | Example |
|-------------|-------------|---------|
| `passthrough` | Copy value unchanged (preserves original type) | IDs, timestamps, non-PII, numeric columns |
| `using(f)` | Apply String => String transformation | `using(FirstName.anonymize)` |
| `setNull` | Replace with null | Sensitive fields to clear |
| `fixed(value)` | Constant replacement (any type) | `fixed("REDACTED")`, `fixed(0)` |
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

Anonymizers are `String => String` functions that produce realistic fake data. They are separate from transformers (like `passthrough`, `setNull`, `fixed`) which handle type preservation.

All anonymizers are in `DeterministicAnonymizer` and implement the `Anonymizer` trait:

```scala
sealed trait Anonymizer {
  def anonymize(input: String): String
}

// Usage in DSL:
"column" -> using(FirstName.anonymize)
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
| `Redact` | Asterisks (preserves length) | "****" |
| `PartialRedact(first, last)` | Partial masking | "Jo**oe" |
| `LoremText` | Lorem ipsum of similar length | "lorem ipsum dolor..." |

> **Note:** `passthrough`, `setNull`, and `fixed(value)` are transformers (not anonymizers) and preserve the original database type. Use them via the DSL: `passthrough`, `setNull`, `fixed("value")`.

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

Database-level validation:

```scala
for {
  result <- sourceDb.run(validateTransformerCoverage("users", transformer))
} yield result match {
  case Left(missing) => println(s"Missing non-PK/FK columns: $missing")
  case Right(())     => println("All data columns covered")
}
```

## Architecture

```
simpleanonymizer/
├── DeterministicAnonymizer.scala  — Anonymization functions using DataFaker
│   ├── Sealed Anonymizer trait (String => String fakers)
│   ├── Hash-based deterministic selection
│   └── Pre-fetched data lists for performance
│
├── RowTransformer.scala           — Composable transformer DSL
│   ├── ResultKind                 — Describes transformation output type
│   │   ├── UseOriginal           — Preserves database type (passthrough)
│   │   ├── SetNull               — Insert null
│   │   ├── UseFixed(value)       — Fixed value of any type
│   │   └── TransformString(f)    — String transformation
│   ├── ValueTransformer           — Leaf-level transformations
│   ├── JsonNav                    — JSON navigation (Direct, Field, ArrayOf)
│   ├── ColumnSpec                 — Column specification with dependencies
│   ├── TableTransformer           — Composes column specs
│   └── DSL                        — User-facing API
│
├── DbSnapshot.scala               — Database operations (Slick-based)
│   ├── getAllTables               — List tables in schema
│   ├── getTableColumns            — List columns in table
│   ├── getDataColumns             — List non-PK/FK columns
│   ├── getPrimaryKeyColumns       — Get PK columns
│   ├── getForeignKeyColumns       — Get FK columns
│   ├── getForeignKeys             — Get all FK relationships
│   ├── validateTransformerCoverage— Validate column coverage
│   ├── generateTableSnippet       — Code snippet for error messages
│   └── copyTable                  — Copy with optional transformation
│
├── Snapshot (class)               — High-level orchestrator
│   └── copy                       — Copy all tables with FK ordering
│                                    (enforces transformer coverage)
│
├── DependencyGraph.scala          — FK dependency analysis
│   ├── computeTableLevels         — Topological sort by FK depth
│   └── groupTablesByLevel         — Group tables for parallel copy
│
└── FilterPropagation.scala        — WHERE clause propagation
    ├── TableConfig                — Per-table configuration
    ├── TableSpec                  — Combined config + transformer
    ├── generateChildWhereClause   — Derive child filter from parent
    └── computeEffectiveFilters    — Propagate filters through FKs
```

## License

Apache 2.0
