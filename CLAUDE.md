# simple-anonymizer

A Scala library for deterministic data anonymization.

## Project Structure

- `simple-anonymizer/` - Main library code
  - `DeterministicAnonymizer.scala` - Anonymizer trait and implementations using DataFaker
  - `RowTransformer.scala` - Composable DSL for table transformations
- `tests/` - ScalaTest test suite

## Build System

This project uses Bleep (https://bleep.build/). Key commands:

```bash
bleep compile         # Compile all projects
bleep test            # Run all tests
bleep test tests      # Run tests for specific project
```

## Dependencies

- **net.datafaker:datafaker** - Realistic fake data generation
- **io.circe::circe-core** and **io.circe::circe-parser** - JSON handling
- **org.scalatest::scalatest** - Testing framework

## Architecture

### DeterministicAnonymizer

Uses MD5 hashing to deterministically select from DataFaker's pre-loaded data lists. This ensures:
- Same input → same output (deterministic)
- Realistic output (curated data)
- One-way transformation (cannot recover original)

### RowTransformer DSL

Composable transformer system:
- `ValueTransformer` - Leaf transformations
- `JsonNav` - JSON navigation (Direct, Field, ArrayOf)
- `ColumnSpec` - Column specs with optional dependencies
- `TableTransformer` - Combines column specs
- `DSL` - User-facing API (`table()`, `using()`, `passthrough`, etc.)

## Code Style

- Scala 2.13 and Scala 3 cross-compilation
- Use `source-layout: cross-pure` for Bleep
- Follow existing patterns in the codebase
