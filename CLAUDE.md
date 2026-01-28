# simple-anonymizer

A Scala library for deterministic data anonymization and database copying.

## Project Structure

- `simple-anonymizer/` - Main library code
  - `DeterministicAnonymizer.scala` - Anonymizer trait and implementations using DataFaker
  - `RowTransformer.scala` - Composable DSL for table transformations
  - `DbSnapshot.scala` - Database copying with transformations
  - `DbMetadata.scala` - Database schema introspection (tables, columns, FKs)
  - `DependencyGraph.scala` - Topological sorting of tables by FK dependencies
  - `FilterPropagation.scala` - WHERE clause propagation through FK relationships
- `tests/` - Unit tests (ScalaTest)
- `integration-tests/` - Integration tests with PostgreSQL via Testcontainers

## Build System

This project uses Bleep (https://bleep.build/). Key commands:

```bash
bleep compile              # Compile all projects
bleep test                 # Run all tests
bleep test tests           # Run unit tests only
bleep test integration-tests  # Run integration tests only
```

## Dependencies

- **net.datafaker:datafaker** - Realistic fake data generation
- **io.circe::circe-core** and **io.circe::circe-parser** - JSON handling
- **org.postgresql:postgresql** - PostgreSQL JDBC driver
- **org.scalatest::scalatest** - Testing framework
- **org.testcontainers:postgresql** - Integration testing (test scope)

## Architecture

### DeterministicAnonymizer

Uses MD5 hashing to deterministically select from DataFaker's pre-loaded data lists. This ensures:
- Same input -> same output (deterministic)
- Realistic output (curated data)
- One-way transformation (cannot recover original)

### RowTransformer DSL

Composable transformer system:
- `ValueTransformer` - Leaf transformations
- `JsonNav` - JSON navigation (Direct, Field, ArrayOf)
- `ColumnSpec` - Column specs with optional dependencies
- `TableTransformer` - Combines column specs
- `DSL` - User-facing API (`table()`, `using()`, `passthrough`, etc.)

### Database Modules

- `DbSnapshot` - Main entry point for copying tables between databases
- `DbMetadata` - Schema queries (tables, columns, primary/foreign keys)
- `DependencyGraph` - Computes table levels for correct copy order
- `FilterPropagation` - Propagates WHERE filters through FK relationships

## Code Style

- Scala 2.13 and Scala 3 cross-compilation
- Use `source-layout: cross-pure` for Bleep
- Follow existing patterns in the codebase
