package simpleanonymizer

import org.scalatest.{BeforeAndAfterAll, FutureOutcome}
import org.scalatest.funspec.FixtureAsyncFunSpec

import SlickProfile.api._

/** Integration tests demonstrating DbCopier API usage.
  *
  * These tests serve as working examples of how to use the library. The focus is on clean, readable code that shows typical usage patterns.
  */
class DbCopierIntegrationTest extends FixtureAsyncFunSpec with BeforeAndAfterAll {

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

  describe("PII anonymization") {
    it("anonymizes names and emails across all tables") { fixture =>
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
        _      <- assert(result("users") == 10)
        _      <- assert(result("categories") == 0)
        _      <- assert(result("orders") > 0)
        _      <- assert(result("profiles") > 0)
        users  <- targetDb.run(sql"SELECT first_name, email FROM users WHERE id = 1".as[(String, String)])
        _      <- assert(users.head._1 != "John", "first_name should be anonymized")
        _      <- assert(!users.head._2.contains("john"), "email should be anonymized")
      } yield succeed
    }

    it("is deterministic - same input always produces same output") { fixture =>
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
        _     <- targetDb.run(
                   sql"""SELECT first_name FROM users WHERE id IN (
                      SELECT id FROM users ORDER BY id
                   )""".as[String]
                 )
        john1 <- targetDb.run(sql"SELECT first_name FROM users WHERE id = 1".as[String])
        _     <- assert(Anonymizer.FirstName("John") == john1.head)
      } yield succeed
    }
  }

  describe("FK propagation") {
    it("filters child tables based on parent WHERE clause") { fixture =>
      val copier   = new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("categories"))
      val targetDb = fixture.targetDb

      for {
        result <- copier.run(
                    "users"       ->
                      TableSpec
                        .select(row => Seq(row.first_name, row.last_name, row.email))
                        .where("id <= 3"),
                    "orders"      -> TableSpec.select(row => Seq(row.status, row.total)),
                    "order_items" -> TableSpec.select(row => Seq(row.product_name, row.quantity)),
                    "profiles"    -> TableSpec.select(row => Seq(row.phones, row.settings))
                  )
        _      <- assert(result("users") == 3)
        orders <- targetDb.run(sql"SELECT DISTINCT user_id FROM orders".as[Int])
        _      <- assert(orders.forall(_ <= 3), s"Only orders for users 1-3 should be copied, got: $orders")
      } yield succeed
    }
  }

  describe("nulled and fixed values") {
    it("nulled clears a column completely") { fixture =>
      val copier   =
        new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("orders", "order_items", "profiles", "categories"))
      val targetDb = fixture.targetDb

      for {
        _      <- copier.run(
                    "users" -> TableSpec.select { row =>
                      Seq(
                        row.first_name,
                        row.last_name,
                        row.email.nulled
                      )
                    }
                  )
        emails <- targetDb.run(sql"SELECT email FROM users WHERE email IS NOT NULL".as[String])
        _      <- assert(emails.isEmpty, "All emails should be null")
      } yield succeed
    }

    it(":= replaces all values with a constant") { fixture =>
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
        emails <- targetDb.run(sql"SELECT DISTINCT email FROM users".as[String])
        _      <- assert(emails.length == 1)
        _      <- assert(emails.head == "redacted@example.com")
      } yield succeed
    }
  }

  describe("JSON column transformation") {
    it("anonymizes fields within JSON arrays while preserving structure") { fixture =>
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
        phones <- targetDb.run(sql"SELECT phones FROM profiles WHERE id = 1".as[String])
        _      <- assert(!phones.head.contains("555-0101"), "Phone numbers should be anonymized")
        _      <- assert(phones.head.contains("type"), "JSON structure should be preserved")
        _      <- assert(phones.head.contains("mobile"), "JSON values not targeted should be preserved")
      } yield succeed
    }
  }

  describe("type preservation") {
    it("preserves DECIMAL and INTEGER types through passthrough") { fixture =>
      val copier   = new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("profiles", "categories"))
      val targetDb = fixture.targetDb

      for {
        _          <- copier.run(
                        "users"       -> TableSpec.select { row =>
                          Seq(row.first_name, row.last_name, row.email)
                        },
                        "orders"      -> TableSpec.select { row =>
                          Seq(row.status, row.total)
                        },
                        "order_items" -> TableSpec.select { row =>
                          Seq(row.product_name, row.quantity)
                        }
                      )
        totals     <- targetDb.run(sql"SELECT total FROM orders WHERE id = 1".as[BigDecimal])
        _          <- assert(totals.head == BigDecimal("299.99"))
        quantities <- targetDb.run(sql"SELECT quantity FROM order_items WHERE id = 2".as[Int])
        _          <- assert(quantities.head == 2)
      } yield succeed
    }
  }

  describe("error messages") {
    they("include code snippets for missing tables") { fixture =>
      val copier = new DbCopier(sourceDb, fixture.targetDb)
      val result = copier.run(
        "users" -> TableSpec.select { row =>
          Seq(row.first_name, row.last_name, row.email)
        }
      )

      recoverToExceptionIf[IllegalArgumentException](result).map { ex =>
        assert(ex.getMessage.contains("Missing table specs"))
        assert(ex.getMessage.contains("-> TableSpec.select { row =>"))
      }
    }

    they("include code snippets for missing columns") { fixture =>
      val copier =
        new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("orders", "order_items", "profiles", "categories"))
      val result = copier.run(
        "users" -> TableSpec.select { row =>
          Seq(row.first_name, row.last_name)
        }
      )

      recoverToExceptionIf[IllegalArgumentException](result).map { ex =>
        assert(ex.getMessage.contains("missing"))
        assert(ex.getMessage.contains("email"))
      }
    }
  }
}
