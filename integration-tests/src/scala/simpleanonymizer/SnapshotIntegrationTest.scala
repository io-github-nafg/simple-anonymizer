package simpleanonymizer

import org.scalatest.{BeforeAndAfterAll, FutureOutcome}
import org.scalatest.funsuite.FixtureAsyncFunSuite
import org.testcontainers.utility.MountableFile
import slick.additions.testcontainers.SlickPostgresContainer

import SlickProfile.api._

/** Integration tests demonstrating Snapshot API usage.
  *
  * These tests serve as working examples of how to use the library. The focus is on clean, readable code that shows typical usage patterns.
  */
class SnapshotIntegrationTest extends FixtureAsyncFunSuite with BeforeAndAfterAll {
  import DbSnapshot.TableConfig
  import DeterministicAnonymizer._
  import RowTransformer.DSL._

  // ---------------------------------------------------------------------------
  // Test infrastructure (skip when reading for examples)
  // ---------------------------------------------------------------------------

  private lazy val sourceContainer: SlickPostgresContainer = {
    val container = new SlickPostgresContainer()
    container.withCopyFileToContainer(
      MountableFile.forClasspathResource("01-schema.sql"),
      "/docker-entrypoint-initdb.d/01-schema.sql"
    )
    container.withCopyFileToContainer(
      MountableFile.forClasspathResource("02-data.sql"),
      "/docker-entrypoint-initdb.d/02-data.sql"
    )
    container
  }

  private lazy val sourceDb: Database = sourceContainer.slickDatabase(SlickProfile.backend)

  override def beforeAll(): Unit = sourceContainer.start()
  override def afterAll(): Unit  = sourceContainer.stop()

  override type FixtureParam = Snapshot

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val targetContainer = new SlickPostgresContainer()
    targetContainer.withCopyFileToContainer(
      MountableFile.forClasspathResource("01-schema.sql"),
      "/docker-entrypoint-initdb.d/01-schema.sql"
    )
    targetContainer.start()
    val targetDb        = targetContainer.slickDatabase(SlickProfile.backend)

    val snapshot = new Snapshot(sourceDb, targetDb)

    complete {
      withFixture(test.toNoArgAsyncTest(snapshot))
    }.lastly {
      try targetDb.close()
      finally targetContainer.stop()
    }
  }

  private def countRows(db: Database, table: String) =
    db.run(sql"SELECT COUNT(*) FROM #$table".as[Int].head)

  // ---------------------------------------------------------------------------
  // Example: Copy entire database with anonymization
  // ---------------------------------------------------------------------------

  test("copy entire database with PII anonymization") { snapshot =>
    // Configure table handling
    val tableConfigs = Map(
      // Filter to only active-ish data
      "users"      -> TableConfig(whereClause = Some("id <= 5")),
      // Skip categories entirely
      "categories" -> TableConfig(skip = true)
    )

    // Define transformers for ALL non-skipped tables
    val transformers = Map(
      "users"       -> table(
        "first_name" -> using(FirstName.anonymize),
        "last_name"  -> using(LastName.anonymize),
        "email"      -> using(Email.anonymize)
      ),
      "orders"      -> table(
        "status" -> passthrough
        // Note: 'total' is numeric, transformers only handle string columns
      ),
      "order_items" -> table(
        "product_name" -> passthrough
        // Note: 'quantity' is integer, transformers only handle string columns
      ),
      "profiles"    -> table(
        "phones"   -> jsonArray("number")(PhoneNumber.anonymize),
        "settings" -> passthrough
      )
    )

    // Execute the snapshot copy
    for {
      result <- snapshot.copy(tableConfigs, transformers)
      // Verify results
      _      <- assert(result("users") == 5, "Should copy 5 users")
      _      <- assert(result("categories") == 0, "Categories should be skipped")
      _      <- assert(result("orders") > 0, "Should copy some orders")
      _      <- assert(result("profiles") > 0, "Should copy some profiles")
    } yield succeed
  }

  // ---------------------------------------------------------------------------
  // Example: Minimal passthrough for non-PII tables
  // ---------------------------------------------------------------------------

  test("passthrough for tables without sensitive data") { snapshot =>
    val tableConfigs = Map(
      "users"       -> TableConfig(skip = true),
      "orders"      -> TableConfig(skip = true),
      "order_items" -> TableConfig(skip = true),
      "profiles"    -> TableConfig(skip = true)
    )

    // Categories has no PII, just passthrough all columns
    val transformers = Map(
      "categories" -> table(
        "name" -> passthrough
      )
    )

    for {
      result <- snapshot.copy(tableConfigs, transformers)
      _      <- assert(result("categories") == 10, "Should copy all 10 categories")
    } yield succeed
  }

  // ---------------------------------------------------------------------------
  // Example: Error messages with helpful snippets
  // ---------------------------------------------------------------------------

  test("missing table shows helpful error with code snippet") { snapshot =>
    // Only define transformer for users, missing others
    val transformers = Map(
      "users" -> table(
        "first_name" -> passthrough,
        "last_name"  -> passthrough,
        "email"      -> passthrough
      )
    )

    val result = snapshot.copy(Map.empty, transformers)

    recoverToExceptionIf[IllegalArgumentException](result).map { ex =>
      // Error message includes copy-pastable code snippets
      assert(ex.getMessage.contains("Missing transformers"))
      assert(ex.getMessage.contains("-> table("))
      assert(ex.getMessage.contains("TableConfig(skip = true)"))
    }
  }

  test("missing column shows helpful error with code snippet") { snapshot =>
    val tableConfigs = Map(
      "orders"      -> TableConfig(skip = true),
      "order_items" -> TableConfig(skip = true),
      "profiles"    -> TableConfig(skip = true),
      "categories"  -> TableConfig(skip = true)
    )

    // Incomplete transformer - missing email column
    val transformers = Map(
      "users" -> table(
        "first_name" -> passthrough,
        "last_name"  -> passthrough
        // missing: "email" -> ...
      )
    )

    val result = snapshot.copy(tableConfigs, transformers)

    recoverToExceptionIf[IllegalArgumentException](result).map { ex =>
      assert(ex.getMessage.contains("missing"))
      assert(ex.getMessage.contains("email"))
      assert(ex.getMessage.contains("passthrough"))
    }
  }
}
