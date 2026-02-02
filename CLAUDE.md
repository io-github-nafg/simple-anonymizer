# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

This project uses Bleep (https://bleep.build/):

```bash
bleep compile                # Compile all projects
bleep test                   # Run all tests
bleep test tests             # Run unit tests only
bleep test integration-tests # Run integration tests (requires Docker)
bleep test tests -o "*SomeTest*"  # Run a single test class
bleep fmt                    # Format code with scalafmt
```

## Project Structure

- `simple-anonymizer/` - Main library code
- `tests/` - Unit tests (ScalaTest)
- `integration-tests/` - Integration tests with PostgreSQL via Testcontainers

## Architecture

### Anonymizer

`Anonymizer` trait extends `String => String`. Implementations use MD5 hashing to deterministically select from DataFaker's pre-loaded data lists. Same input always produces same output, enabling referential integrity across tables. Pass directly to `mapString`: `row.email.mapString(Anonymizer.Email)`.

### Core Types

- `OutputColumn` - Column output specification (SourceColumn, TransformedColumn, FixedColumn)
- `Lens` - JSON navigation for transforming fields within JSON columns (Direct, Field, ArrayElements)
- `TableSpec` - Combines output columns with optional WHERE clause
- `RawRow` - Row representation with both objects and strings maps

### DSL (on TableSpec companion)

User-facing API using `TableSpec.select { row => Seq(...) }` syntax:
- `row.column` - Passthrough (preserves original type)
- `row.column.mapString(f)` - Apply String => String transformation (null-preserving)
- `row.column.mapOptString(f)` - Apply Option[String] => Option[String] transformation
- `row.column.nulled` - Set to null
- `row.column := value` - Fixed value of any type
- `row.column.mapJsonArray(_.field.mapString(f))` - JSON array transformation

### Database Modules

- `DbCopier` - Copy tables between databases with optional transformations
- `DbMetadata` - Database schema introspection (tables, columns, keys)
- `TableSorter` - Topological sort for correct FK-respecting copy order
- `FilterPropagation` - Propagate WHERE clauses through FK relationships

## Code Style

- Scala 2.13 and Scala 3 cross-compilation
- Use `source-layout: cross-pure` for Bleep
- Always run `bleep fmt` after editing code
