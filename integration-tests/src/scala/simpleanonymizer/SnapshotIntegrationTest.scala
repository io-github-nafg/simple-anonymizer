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
  import DbSnapshot.TableSpec._
  import DeterministicAnonymizer._
  import RowTransformer.DSL._

  // ---------------------------------------------------------------------------
  // Test infrastructure
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

  case class FixtureParam(snapshot: Snapshot, targetDb: Database)

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val targetContainer = new SlickPostgresContainer()
    targetContainer.withCopyFileToContainer(
      MountableFile.forClasspathResource("01-schema.sql"),
      "/docker-entrypoint-initdb.d/01-schema.sql"
    )
    targetContainer.start()
    val targetDb        = targetContainer.slickDatabase(SlickProfile.backend)
    val snapshot        = new Snapshot(sourceDb, targetDb)

    complete {
      withFixture(test.toNoArgAsyncTest(FixtureParam(snapshot, targetDb)))
    }.lastly {
      try targetDb.close()
      finally targetContainer.stop()
    }
  }

  // ---------------------------------------------------------------------------
  // Full database copy with PII anonymization
  // ---------------------------------------------------------------------------

  test("copy entire database with PII anonymization") { fixture =>
    val snapshot = fixture.snapshot
    val targetDb = fixture.targetDb

    for {
      result <- snapshot.copy(
                  "users"       -> copy(
                    table(
                      "first_name" -> using(FirstName.anonymize),
                      "last_name"  -> using(LastName.anonymize),
                      "email"      -> using(Email.anonymize)
                    ),
                    where = "id <= 10"
                  ),
                  "orders"      -> copy(
                    table(
                      "status" -> passthrough,
                      "total"  -> passthrough
                    )
                  ),
                  "order_items" -> copy(
                    table(
                      "product_name" -> passthrough,
                      "quantity"     -> passthrough
                    )
                  ),
                  "profiles"    -> copy(
                    table(
                      "phones"   -> jsonArray("number")(PhoneNumber.anonymize),
                      "settings" -> passthrough
                    )
                  ),
                  "categories"  -> skip
                )
      // Verify row counts
      _      <- assert(result("users") == 10)
      _      <- assert(result("categories") == 0)
      _      <- assert(result("orders") > 0)
      _      <- assert(result("profiles") > 0)
      // Verify data was actually anonymized
      users  <- targetDb.run(sql"SELECT first_name, email FROM users WHERE id = 1".as[(String, String)])
      _      <- assert(users.head._1 != "John", "first_name should be anonymized")
      _      <- assert(!users.head._2.contains("john"), "email should be anonymized")
    } yield succeed
  }

  // ---------------------------------------------------------------------------
  // Subsetting with FK propagation
  // ---------------------------------------------------------------------------

  test("FK propagation filters child tables based on parent filter") { fixture =>
    val snapshot = fixture.snapshot
    val targetDb = fixture.targetDb

    for {
      result <- snapshot.copy(
                  // Only copy first 3 users
                  "users"       -> copy(
                    table(
                      "first_name" -> passthrough,
                      "last_name"  -> passthrough,
                      "email"      -> passthrough
                    ),
                    where = "id <= 3"
                  ),
                  // Orders will be auto-filtered to only those for users 1-3
                  "orders"      -> copy(
                    table(
                      "status" -> passthrough,
                      "total"  -> passthrough
                    )
                  ),
                  // Order items filtered through orders
                  "order_items" -> copy(
                    table(
                      "product_name" -> passthrough,
                      "quantity"     -> passthrough
                    )
                  ),
                  "profiles"    -> copy(
                    table(
                      "phones"   -> passthrough,
                      "settings" -> passthrough
                    )
                  ),
                  "categories"  -> skip
                )
      _      <- assert(result("users") == 3)
      // Verify only orders for users 1-3 were copied
      orders <- targetDb.run(sql"SELECT DISTINCT user_id FROM orders".as[Int])
      _      <- assert(orders.forall(_ <= 3), s"Only orders for users 1-3 should be copied, got: $orders")
    } yield succeed
  }

  // ---------------------------------------------------------------------------
  // Using setNull and fixed values
  // ---------------------------------------------------------------------------

  test("setNull clears sensitive columns") { fixture =>
    val snapshot = fixture.snapshot
    val targetDb = fixture.targetDb

    for {
      _      <- snapshot.copy(
                  "users"       -> copy(
                    table(
                      "first_name" -> passthrough,
                      "last_name"  -> passthrough,
                      "email"      -> setNull // Clear email completely
                    )
                  ),
                  "orders"      -> skip,
                  "order_items" -> skip,
                  "profiles"    -> skip,
                  "categories"  -> skip
                )
      // Verify emails are null
      emails <- targetDb.run(sql"SELECT email FROM users WHERE email IS NOT NULL".as[String])
      _      <- assert(emails.isEmpty, "All emails should be null")
    } yield succeed
  }

  test("fixed replaces values with constants") { fixture =>
    val snapshot = fixture.snapshot
    val targetDb = fixture.targetDb

    for {
      _      <- snapshot.copy(
                  "users"       -> copy(
                    table(
                      "first_name" -> passthrough,
                      "last_name"  -> passthrough,
                      "email"      -> fixed("redacted@example.com")
                    )
                  ),
                  "orders"      -> skip,
                  "order_items" -> skip,
                  "profiles"    -> skip,
                  "categories"  -> skip
                )
      // Verify all emails are the fixed value
      emails <- targetDb.run(sql"SELECT DISTINCT email FROM users".as[String])
      _      <- assert(emails.length == 1)
      _      <- assert(emails.head == "redacted@example.com")
    } yield succeed
  }

  // ---------------------------------------------------------------------------
  // JSON column transformation
  // ---------------------------------------------------------------------------

  test("jsonArray anonymizes fields within JSON arrays") { fixture =>
    val snapshot = fixture.snapshot
    val targetDb = fixture.targetDb

    for {
      _      <- snapshot.copy(
                  "users"       -> copy(
                    table(
                      "first_name" -> passthrough,
                      "last_name"  -> passthrough,
                      "email"      -> passthrough
                    )
                  ),
                  "orders"      -> skip,
                  "order_items" -> skip,
                  "profiles"    -> copy(
                    table(
                      "phones"   -> jsonArray("number")(PhoneNumber.anonymize),
                      "settings" -> passthrough
                    )
                  ),
                  "categories"  -> skip
                )
      // Verify phone numbers were anonymized
      phones <- targetDb.run(sql"SELECT phones FROM profiles WHERE id = 1".as[String])
      _      <- assert(!phones.head.contains("555-0101"), "Phone numbers should be anonymized")
      // Verify JSON structure is preserved
      _      <- assert(phones.head.contains("type"), "JSON structure should be preserved")
      _      <- assert(phones.head.contains("mobile"), "JSON values not targeted should be preserved")
    } yield succeed
  }

  // ---------------------------------------------------------------------------
  // Type preservation
  // ---------------------------------------------------------------------------

  test("passthrough preserves DECIMAL and INTEGER types") { fixture =>
    val snapshot = fixture.snapshot
    val targetDb = fixture.targetDb

    for {
      _      <- snapshot.copy(
                  "users"       -> copy(
                    table(
                      "first_name" -> passthrough,
                      "last_name"  -> passthrough,
                      "email"      -> passthrough
                    )
                  ),
                  "orders"      -> copy(
                    table(
                      "status" -> passthrough,
                      "total"  -> passthrough // DECIMAL(10,2)
                    )
                  ),
                  "order_items" -> copy(
                    table(
                      "product_name" -> passthrough,
                      "quantity"     -> passthrough // INTEGER
                    )
                  ),
                  "profiles"    -> skip,
                  "categories"  -> skip
                )
      // Verify DECIMAL precision is preserved
      totals <- targetDb.run(sql"SELECT total FROM orders WHERE id = 1".as[BigDecimal])
      _      <- assert(totals.head == BigDecimal("299.99"))
      // Verify INTEGER is preserved
      qtys   <- targetDb.run(sql"SELECT quantity FROM order_items WHERE id = 2".as[Int])
      _      <- assert(qtys.head == 2)
    } yield succeed
  }

  // ---------------------------------------------------------------------------
  // Error handling
  // ---------------------------------------------------------------------------

  test("missing table shows helpful error with code snippet") { fixture =>
    val result = fixture.snapshot.copy(
      "users" -> copy(
        table(
          "first_name" -> passthrough,
          "last_name"  -> passthrough,
          "email"      -> passthrough
        )
      )
      // Missing: orders, order_items, profiles, categories

    )

    recoverToExceptionIf[IllegalArgumentException](result).map { ex =>
      assert(ex.getMessage.contains("Missing transformers"))
      assert(ex.getMessage.contains("-> table("))
    }
  }

  test("missing column shows helpful error with code snippet") { fixture =>
    val result = fixture.snapshot.copy(
      "users"       -> copy(
        table(
          "first_name" -> passthrough,
          "last_name"  -> passthrough
          // Missing: email
        )
      ),
      "orders"      -> skip,
      "order_items" -> skip,
      "profiles"    -> skip,
      "categories"  -> skip
    )

    recoverToExceptionIf[IllegalArgumentException](result).map { ex =>
      assert(ex.getMessage.contains("missing"))
      assert(ex.getMessage.contains("email"))
    }
  }

  // ---------------------------------------------------------------------------
  // Deterministic anonymization
  // ---------------------------------------------------------------------------

  test("anonymization is deterministic - same input produces same output") { fixture =>
    val snapshot = fixture.snapshot
    val targetDb = fixture.targetDb

    for {
      _     <- snapshot.copy(
                 "users"       -> copy(
                   table(
                     "first_name" -> using(FirstName.anonymize),
                     "last_name"  -> using(LastName.anonymize),
                     "email"      -> using(Email.anonymize)
                   )
                 ),
                 "orders"      -> skip,
                 "order_items" -> skip,
                 "profiles"    -> skip,
                 "categories"  -> skip
               )
      // Get anonymized names for same original names
      _     <- targetDb.run(
                 sql"""SELECT first_name FROM users WHERE id IN (
                    SELECT id FROM users ORDER BY id
                 )""".as[String]
               )
      // Verify same original name "John" always produces same anonymized name
      // (users with id=1 had first_name "John")
      john1 <- targetDb.run(sql"SELECT first_name FROM users WHERE id = 1".as[String])
      // Running anonymization again on "John" should produce the same result
      _     <- assert(FirstName.anonymize("John") == john1.head)
    } yield succeed
  }
}
