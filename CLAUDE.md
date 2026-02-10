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
bleep publish -- --mode=local  # Publish to local Ivy repository
```

## Project Structure

- `simple-anonymizer/` - Main library (package: `simpleanonymizer`)
- `tests/` - Unit tests (ScalaTest)
- `integration-tests/` - Integration tests with PostgreSQL via Testcontainers
- `scripts/` - Bleep scripts project (publish plugin)

## Architecture

### Data Flow

`DbCopier.run()` orchestrates the full pipeline:
1. `DbContext` introspects source schema (tables, FKs)
2. `TableSorter` topologically sorts tables by FK dependencies (returns levels)
3. `FilterPropagation` propagates WHERE clauses from parent to child tables via FK-based IN subqueries
4. `CoverageValidator` ensures all tables and non-PK/non-FK columns have specs
5. Tables within each level are copied in parallel via `Future.traverse`; levels are processed sequentially to respect FK ordering

### DSL Mechanics

`TableSpec.select { row => Seq(...) }` uses Scala's `Dynamic` trait on `Row` — `row.column_name` resolves to `OutputColumn.SourceColumn(name)` via `selectDynamic`. The same Dynamic pattern is reused for JSON object field access in `mapJsonArray`.

### OutputColumn Variants

- `SourceColumn(name)` — Passthrough; returns raw object from `RawRow.objects` preserving original type (important for DECIMAL, JSON, etc.)
- `TransformedColumn(name, lens, f)` — Applies `f: RawRow => Option[String] => Option[String]` through a `Lens` for JSON navigation
- `FixedColumn(name, value)` — Always returns constant value

### Dual Representation (RawRow)

`RawRow` holds both `objects: Map[String, AnyRef]` and `strings: Map[String, String]`. Objects preserve DB types for passthrough columns; strings enable transformation functions. `CopyAction.BatchInserter` pre-computes a `writers: Vector[RawRow => AnyRef]` array to avoid per-row pattern matching.

### Lens System

`Lens` navigates JSON structures: `Direct` (no parsing), `Field(name, inner)` (object field), `ArrayElements(lens)` (iterate array). Composed by `mapJsonArray` DSL. When `Direct` is nested inside JSON lenses, it extracts/injects string values from JSON nodes.

### Anonymizer

`Anonymizer` extends `String => String`. Uses MD5 hashing (`stableHash`) for deterministic selection from DataFaker's pre-loaded data lists. Same input always produces same output across tables, preserving referential integrity.

### Filter Propagation

Processes tables in topological order. For child tables with FK to a filtered parent, generates: `child_fk_col IN (SELECT parent_pk FROM parent WHERE parent_filter)`. Transitive — grandchild tables inherit through the chain.

### CoverageValidator

Requires explicit handling for all non-PK, non-FK columns. Generates copy-pastable code snippets in error messages showing missing table/column specs.

## Code Style

- Scala 2.13 and Scala 3 cross-compilation
- Use `source-layout: cross-pure` for Bleep
- Always run `bleep fmt` after editing code

## Publishing

- Config in `bleep.publish.yaml` (group ID, projects, Sonatype config, metadata)
- Version derived from git tags via dynver (tag `v0.1.0` → version `0.1.0`)
- CI publishes to Maven Central on `v*` tag push via GitHub Actions
- Requires secrets: `PGP_PASSPHRASE`, `PGP_SECRET`, `SONATYPE_USERNAME`, `SONATYPE_PASSWORD`
