package simpleanonymizer

import org.scalatest.{BeforeAndAfterAll, FutureOutcome}
import org.scalatest.funsuite.FixtureAsyncFunSuite

import SlickProfile.api._

/** Integration tests demonstrating DbCopier API usage.
  *
  * These tests serve as working examples of how to use the library. The focus is on clean, readable code that shows typical usage patterns.
  */
class DbCopierIntegrationTest extends FixtureAsyncFunSuite with BeforeAndAfterAll {

  private lazy val sourceContainer = PostgresTestBase.createContainer()
  private lazy val sourceDb        = sourceContainer.slickDatabase(SlickProfile.backend)

  override def beforeAll(): Unit = sourceContainer.start()
  override def afterAll(): Unit  = sourceContainer.stop()

  case class FixtureParam(targetDb: Database)

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val targetContainer = PostgresTestBase.createEmptyContainer()
    targetContainer.start()
    val targetDb        = targetContainer.slickDatabase(SlickProfile.backend)

    complete {
      withFixture(test.toNoArgAsyncTest(FixtureParam(targetDb)))
    }.lastly {
      try targetDb.close()
      finally targetContainer.stop()
    }
  }

  // ---------------------------------------------------------------------------
  // Full database copy with PII anonymization
  // ---------------------------------------------------------------------------

  test("copy entire database with PII anonymization") { fixture =>
    val copier   = new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("categories"))
    val targetDb = fixture.targetDb

    for {
      result <- copier.run(
                  "users"       -> TableSpec
                    .select { row =>
                      Seq(
                        row.first_name.mapString(Anonymizer.FirstName),
                        row.last_name.mapString(Anonymizer.LastName),
                        row.email.mapString(Anonymizer.Email)
                      )
                    }
                    .where("id <= 10"),
                  "orders"      -> TableSpec.select { row =>
                    Seq(row.status, row.total)
                  },
                  "order_items" -> TableSpec.select { row =>
                    Seq(row.product_name, row.quantity)
                  },
                  "profiles"    -> TableSpec.select { row =>
                    Seq(
                      row.phones.mapJsonArray(_.number.mapString(Anonymizer.PhoneNumber)),
                      row.settings
                    )
                  }
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
    val copier   = new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("categories"))
    val targetDb = fixture.targetDb

    for {
      result <- copier.run(
                  // Only copy the first 3 users
                  "users"       ->
                    TableSpec
                      .select(row => Seq(row.first_name, row.last_name, row.email))
                      .where("id <= 3"),
                  // Orders will be auto-filtered to only those for users 1-3
                  "orders"      -> TableSpec.select(row => Seq(row.status, row.total)),
                  // Order items filtered through orders
                  "order_items" -> TableSpec.select(row => Seq(row.product_name, row.quantity)),
                  "profiles"    -> TableSpec.select(row => Seq(row.phones, row.settings))
                )
      _      <- assert(result("users") == 3)
      // Verify only orders for users 1-3 were copied
      orders <- targetDb.run(sql"SELECT DISTINCT user_id FROM orders".as[Int])
      _      <- assert(orders.forall(_ <= 3), s"Only orders for users 1-3 should be copied, got: $orders")
    } yield succeed
  }

  // ---------------------------------------------------------------------------
  // Using nulled and fixed values
  // ---------------------------------------------------------------------------

  test("nulled clears sensitive columns") { fixture =>
    val copier   =
      new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("orders", "order_items", "profiles", "categories"))
    val targetDb = fixture.targetDb

    for {
      _      <- copier.run(
                  "users" -> TableSpec.select { row =>
                    Seq(
                      row.first_name,
                      row.last_name,
                      row.email.nulled // Clear email completely
                    )
                  }
                )
      // Verify emails are null
      emails <- targetDb.run(sql"SELECT email FROM users WHERE email IS NOT NULL".as[String])
      _      <- assert(emails.isEmpty, "All emails should be null")
    } yield succeed
  }

  test("fixed replaces values with constants") { fixture =>
    val copier   =
      new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("orders", "order_items", "profiles", "categories"))
    val targetDb = fixture.targetDb

    for {
      _      <- copier.run(
                  "users" -> TableSpec.select { row =>
                    Seq(
                      row.first_name,
                      row.email := "redacted@example.com",
                      row.last_name
                    )
                  }
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
    val copier   =
      new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("orders", "order_items", "categories"))
    val targetDb = fixture.targetDb

    for {
      _      <- copier.run(
                  "users"    -> TableSpec.select { row =>
                    Seq(row.first_name, row.last_name, row.email)
                  },
                  "profiles" -> TableSpec.select { row =>
                    Seq(
                      row.phones.mapJsonArray(_.number.mapString(Anonymizer.PhoneNumber)),
                      row.settings
                    )
                  }
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
    val copier   = new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("profiles", "categories"))
    val targetDb = fixture.targetDb

    for {
      _          <- copier.run(
                      "users"       -> TableSpec.select { row =>
                        Seq(row.first_name, row.last_name, row.email)
                      },
                      "orders"      -> TableSpec.select { row =>
                        Seq(row.status, row.total) // DECIMAL(10,2)
                      },
                      "order_items" -> TableSpec.select { row =>
                        Seq(row.product_name, row.quantity) // INTEGER
                      }
                    )
      // Verify DECIMAL precision is preserved
      totals     <- targetDb.run(sql"SELECT total FROM orders WHERE id = 1".as[BigDecimal])
      _          <- assert(totals.head == BigDecimal("299.99"))
      // Verify INTEGER is preserved
      quantities <- targetDb.run(sql"SELECT quantity FROM order_items WHERE id = 2".as[Int])
      _          <- assert(quantities.head == 2)
    } yield succeed
  }

  // ---------------------------------------------------------------------------
  // Error handling
  // ---------------------------------------------------------------------------

  test("missing table shows helpful error with code snippet") { fixture =>
    val copier = new DbCopier(sourceDb, fixture.targetDb)
    val result = copier.run(
      "users" -> TableSpec.select { row =>
        Seq(row.first_name, row.last_name, row.email)
      }
      // Missing: orders, order_items, profiles, categories
    )

    recoverToExceptionIf[IllegalArgumentException](result).map { ex =>
      assert(ex.getMessage.contains("Missing table specs"))
      assert(ex.getMessage.contains("-> TableSpec.select { row =>"))
    }
  }

  test("missing column shows helpful error with code snippet") { fixture =>
    val copier =
      new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("orders", "order_items", "profiles", "categories"))
    val result = copier.run(
      "users" -> TableSpec.select { row =>
        Seq(row.first_name, row.last_name)
      // Missing: email
      }
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
    val copier   =
      new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("orders", "order_items", "profiles", "categories"))
    val targetDb = fixture.targetDb

    for {
      _     <- copier.run(
                 "users" -> TableSpec.select { row =>
                   Seq(
                     row.first_name.mapString(Anonymizer.FirstName),
                     row.last_name.mapString(Anonymizer.LastName),
                     row.email.mapString(Anonymizer.Email)
                   )
                 }
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
      _     <- assert(Anonymizer.FirstName("John") == john1.head)
    } yield succeed
  }
}
