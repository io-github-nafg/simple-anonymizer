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

### DeterministicAnonymizer

Uses MD5 hashing to deterministically select from DataFaker's pre-loaded data lists. Same input always produces same output, enabling referential integrity across tables.

### RowTransformer DSL

Composable transformer system with layered abstractions:
- `ValueTransformer` - Leaf transformations (e.g., anonymize a string)
- `JsonNav` - JSON navigation for transforming fields within JSON columns
- `ColumnSpec` - Column specifications, optionally depending on other columns
- `TableTransformer` - Combines column specs into a row transformer
- `DSL` - User-facing API (`table()`, `using()`, `passthrough`, `jsonArray()`, `col()`)

### Database Modules

- `DbSnapshot` - Copy tables between databases with optional transformations
- `DbMetadata` - Schema introspection (tables, columns, primary/foreign keys)
- `DependencyGraph` - Topological sort for correct FK-respecting copy order
- `FilterPropagation` - Propagate WHERE clauses through FK relationships

## Code Style

- Scala 2.13 and Scala 3 cross-compilation
- Use `source-layout: cross-pure` for Bleep
- Always run `bleep fmt` after editing code