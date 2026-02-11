package simpleanonymizer

import org.scalatest.{BeforeAndAfterAll, FutureOutcome}
import org.scalatest.funspec.FixtureAsyncFunSpec

import SlickProfile.api._

/** Integration tests demonstrating DbCopier API usage.
  *
  * These tests serve as working examples of how to use the library. The focus is on clean, readable code that shows typical usage patterns. Keep specs explicit
  * to reflect realistic usage.
  */
class DbCopierIntegrationTest extends FixtureAsyncFunSpec with BeforeAndAfterAll {

  private lazy val sourceDb = PostgresTestBase.sourceContainer.slickDatabase(SlickProfile.backend)

  override def afterAll(): Unit = sourceDb.close()

  case class FixtureParam(targetDb: Database)

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val (targetDb, dbName) = PostgresTestBase.createTargetDb()

    complete {
      withFixture(test.toNoArgAsyncTest(FixtureParam(targetDb)))
    }.lastly {
      try targetDb.close()
      finally PostgresTestBase.dropTargetDb(dbName)
    }
  }

  describe("PII anonymization") {
    it("anonymizes names and emails across all tables") { fixture =>
      val copier   = new DbCopier(sourceDb, fixture.targetDb)
      val targetDb = fixture.targetDb

      for {
        result             <- copier.run(
                                "users"       -> TableSpec
                                  .select { row =>
                                    Seq(
                                      row.first_name.mapString(Anonymizer.FirstName),
                                      row.last_name.mapString(Anonymizer.LastName),
                                      row.email.mapString(Anonymizer.Email)
                                    )
                                  }
                                  .where("id <= 10"),
                                "orders"      -> TableSpec.select(row => Seq(row.status, row.total)),
                                "order_items" -> TableSpec.select(row => Seq(row.product_name, row.quantity)),
                                "profiles"    -> TableSpec.select { row =>
                                  Seq(
                                    row.phones.mapJsonArray(_.number.mapString(Anonymizer.PhoneNumber)),
                                    row.settings
                                  )
                                },
                                "categories"  -> TableSpec.select(row => Seq(row.name)),
                                "employees"   -> TableSpec.select(row => Seq(row.name)),
                                "tree_nodes"  -> TableSpec.select(row => Seq(row.label))
                              )
        _                  <- assert(result("users") == 10)
        _                  <- assert(result("categories") == 10)
        _                  <- assert(result("orders") > 0)
        _                  <- assert(result("profiles") > 0)
        (firstName, email) <- targetDb.run(sql"SELECT first_name, email FROM users WHERE id = 1".as[(String, String)].head)
        _                  <- assert(firstName != "John", "first_name should be anonymized")
        _                  <- assert(!email.contains("john"), "email should be anonymized")
      } yield succeed
    }

    it("is deterministic - same input always produces same output") { fixture =>
      val copier   =
        new DbCopier(
          sourceDb,
          fixture.targetDb,
          skippedTables = Set("orders", "order_items", "profiles", "employees", "tree_nodes")
        )
      val targetDb = fixture.targetDb

      for {
        _     <- copier.run(
                   "users"      -> TableSpec.select { row =>
                     Seq(
                       row.first_name.mapString(Anonymizer.FirstName),
                       row.last_name.mapString(Anonymizer.LastName),
                       row.email.mapString(Anonymizer.Email)
                     )
                   },
                   "categories" -> TableSpec.select(row => Seq(row.name))
                 )
        john1 <- targetDb.run(sql"SELECT first_name FROM users WHERE id = 1".as[String])
        _     <- assert(Anonymizer.FirstName("John") == john1.head)
      } yield succeed
    }
  }

  describe("FK propagation") {
    it("filters child tables based on parent WHERE clause") { fixture =>
      val copier   = new DbCopier(sourceDb, fixture.targetDb)
      val targetDb = fixture.targetDb

      for {
        result <- copier.run(
                    "users"       ->
                      TableSpec
                        .select(row => Seq(row.first_name, row.last_name, row.email))
                        .where("id <= 3"),
                    "orders"      -> TableSpec.select(row => Seq(row.status, row.total)),
                    "order_items" -> TableSpec.select(row => Seq(row.product_name, row.quantity)),
                    "profiles"    -> TableSpec.select(row => Seq(row.phones, row.settings)),
                    "categories"  -> TableSpec.select(row => Seq(row.name)),
                    "employees"   -> TableSpec.select(row => Seq(row.name)),
                    "tree_nodes"  -> TableSpec.select(row => Seq(row.label))
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
        new DbCopier(
          sourceDb,
          fixture.targetDb,
          skippedTables = Set("orders", "order_items", "profiles", "employees", "tree_nodes")
        )
      val targetDb = fixture.targetDb

      for {
        _      <- copier.run(
                    "users"      -> TableSpec.select { row =>
                      Seq(
                        row.first_name,
                        row.last_name,
                        row.email.nulled
                      )
                    },
                    "categories" -> TableSpec.select(row => Seq(row.name))
                  )
        emails <- targetDb.run(sql"SELECT email FROM users WHERE email IS NOT NULL".as[String])
        _      <- assert(emails.isEmpty, "All emails should be null")
      } yield succeed
    }

    it(":= replaces all values with a constant") { fixture =>
      val copier   =
        new DbCopier(
          sourceDb,
          fixture.targetDb,
          skippedTables = Set("orders", "order_items", "profiles", "employees", "tree_nodes")
        )
      val targetDb = fixture.targetDb

      for {
        _      <- copier.run(
                    "users"      -> TableSpec.select { row =>
                      Seq(
                        row.first_name,
                        row.email := "redacted@example.com",
                        row.last_name
                      )
                    },
                    "categories" -> TableSpec.select(row => Seq(row.name))
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
        new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("orders", "order_items", "employees", "tree_nodes"))
      val targetDb = fixture.targetDb

      for {
        _      <- copier.run(
                    "users"      -> TableSpec.select { row =>
                      Seq(row.first_name, row.last_name, row.email)
                    },
                    "profiles"   -> TableSpec.select { row =>
                      Seq(
                        row.phones.mapJsonArray(_.number.mapString(Anonymizer.PhoneNumber)),
                        row.settings
                      )
                    },
                    "categories" -> TableSpec.select(row => Seq(row.name))
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
      val copier   = new DbCopier(sourceDb, fixture.targetDb, skippedTables = Set("profiles", "employees", "tree_nodes"))
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
                        },
                        "categories"  -> TableSpec.select(row => Seq(row.name))
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
        new DbCopier(
          sourceDb,
          fixture.targetDb,
          skippedTables = Set("orders", "order_items", "profiles", "employees", "tree_nodes")
        )
      val result = copier.run(
        "users"      -> TableSpec.select { row =>
          Seq(row.first_name, row.last_name)
        },
        "categories" -> TableSpec.select(row => Seq(row.name))
      )

      recoverToExceptionIf[IllegalArgumentException](result).map { ex =>
        assert(ex.getMessage.contains("missing"))
        assert(ex.getMessage.contains("email"))
      }
    }
  }

  describe("onConflict") {
    it("with doUpdate updates existing rows on primary key conflict") { fixture =>
      val copier   =
        new DbCopier(
          sourceDb,
          fixture.targetDb,
          skippedTables = Set("orders", "order_items", "profiles", "employees", "tree_nodes")
        )
      val targetDb = fixture.targetDb

      for {
        // First copy: initial data
        _              <- copier.run(
                            "categories" -> TableSpec.select(row => Seq(row.name)),
                            "users"      ->
                              TableSpec
                                .select(row => Seq(row.first_name, row.last_name, row.email))
                                .where("id = 1")
                          )
        originalName   <- targetDb.run(sql"SELECT first_name FROM users WHERE id = 1".as[String].head)
        _              <- assert(originalName == "John")
        // Second copy with doUpdate: should update, not fail
        _              <- copier.run(
                            "categories" ->
                              TableSpec
                                .select(row => Seq(row.name))
                                .onConflict(OnConflict.doNothing),
                            "users"      ->
                              TableSpec
                                .select(row => Seq(row.first_name.mapString(_ => "UPDATED"), row.last_name, row.email))
                                .where("id = 1")
                                .onConflict(OnConflict.doUpdate)
                          )
        updatedName    <- targetDb.run(sql"SELECT first_name FROM users WHERE id = 1".as[String].head)
        _              <- assert(updatedName == "UPDATED")
        // Verify count didn't change (no duplicate)
        userCount      <- targetDb.run(sql"SELECT COUNT(*) FROM users WHERE id = 1".as[Int].head)
        _              <- assert(userCount == 1)
        // Verify categories weren't duplicated
        categoryCount  <- targetDb.run(sql"SELECT COUNT(*) FROM categories".as[Int].head)
        originalCatCnt <- sourceDb.run(sql"SELECT COUNT(*) FROM categories".as[Int].head)
        _              <- assert(categoryCount == originalCatCnt)
      } yield succeed
    }

    it("with doNothing skips conflicting rows on primary key conflict") { fixture =>
      val copier   =
        new DbCopier(
          sourceDb,
          fixture.targetDb,
          skippedTables = Set("orders", "order_items", "profiles", "employees", "tree_nodes")
        )
      val targetDb = fixture.targetDb

      for {
        // First copy
        _           <- copier.run(
                         "categories" -> TableSpec.select(row => Seq(row.name)),
                         "users"      ->
                           TableSpec
                             .select(row => Seq(row.first_name, row.last_name, row.email))
                             .where("id <= 2")
                       )
        count1      <- targetDb.run(sql"SELECT COUNT(*) FROM users".as[Int].head)
        _           <- assert(count1 == 2)
        // Second copy with doNothing: existing rows skipped, new rows added
        _           <- copier.run(
                         "categories" -> TableSpec.select(row => Seq(row.name)).onConflict(OnConflict.doNothing),
                         "users"      ->
                           TableSpec
                             .select(row => Seq(row.first_name, row.last_name, row.email))
                             .where("id <= 4")
                             .onConflict(OnConflict.doNothing)
                       )
        count2      <- targetDb.run(sql"SELECT COUNT(*) FROM users".as[Int].head)
        _           <- assert(count2 == 4) // 2 existing + 2 new
        // Original data unchanged (not overwritten)
        originalRow <- targetDb.run(sql"SELECT first_name FROM users WHERE id = 1".as[String].head)
        _           <- assert(originalRow == "John")
      } yield succeed
    }

  }

  describe("sequence reset") {
    it("allows inserting rows without explicit ID after copy") { fixture =>
      val copier   =
        new DbCopier(
          sourceDb,
          fixture.targetDb,
          skippedTables = Set("orders", "order_items", "profiles", "employees", "tree_nodes")
        )
      val targetDb = fixture.targetDb

      for {
        _        <- copier.run(
                      "users"      -> TableSpec.select(row => Seq(row.first_name, row.last_name, row.email)),
                      "categories" -> TableSpec.select(row => Seq(row.name))
                    )
        maxCopId <- targetDb.run(sql"SELECT MAX(id) FROM users".as[Int].head)
        // Insert without explicit ID — should use sequence default and not conflict
        newId    <- targetDb.run(sql"INSERT INTO users (first_name, last_name, email) VALUES ('New', 'User', 'new@test.com') RETURNING id".as[Int].head)
        _        <- assert(newId > maxCopId, s"New ID ($newId) should be > max copied ID ($maxCopId)")
      } yield succeed
    }

    it("allows inserting rows without explicit ID on tables with gaps") { fixture =>
      val copier   =
        new DbCopier(
          sourceDb,
          fixture.targetDb,
          skippedTables = Set("order_items", "profiles", "employees", "tree_nodes")
        )
      val targetDb = fixture.targetDb

      for {
        _        <- copier.run(
                      "users"      ->
                        TableSpec
                          .select(row => Seq(row.first_name, row.last_name, row.email))
                          .where("id <= 3"),
                      "orders"     -> TableSpec.select(row => Seq(row.status, row.total)),
                      "categories" -> TableSpec.select(row => Seq(row.name))
                    )
        maxCopId <- targetDb.run(sql"SELECT MAX(id) FROM users".as[Int].head)
        newId    <- targetDb.run(sql"INSERT INTO users (first_name, last_name, email) VALUES ('Another', 'User', 'another@test.com') RETURNING id".as[Int].head)
        _        <- assert(newId > maxCopId, s"New ID ($newId) should be > max copied ID ($maxCopId)")
      } yield succeed
    }
  }

  describe("limit") {
    it("restricts the number of rows copied") { fixture =>
      val copier   =
        new DbCopier(
          sourceDb,
          fixture.targetDb,
          skippedTables = Set("orders", "order_items", "profiles", "categories", "employees", "tree_nodes")
        )
      val targetDb = fixture.targetDb

      for {
        result    <- copier.run(
                       "users" -> TableSpec
                         .select(row => Seq(row.first_name, row.last_name, row.email))
                         .withLimit(3)
                         .withBatchSize(2)
                     )
        _         <- assert(result("users") == 3)
        userCount <- targetDb.run(sql"SELECT COUNT(*) FROM users".as[Int].head)
        _         <- assert(userCount == 3)
      } yield succeed
    }
  }
}
