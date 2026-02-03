package simpleanonymizer

import org.scalatest.funspec.FixtureAsyncFunSpec
import org.scalatest.{BeforeAndAfterAll, FutureOutcome}
import simpleanonymizer.SlickProfile.api._

class TableCopierIntegrationTest extends FixtureAsyncFunSpec with BeforeAndAfterAll {

  protected val schema: String = "public"

  protected lazy val sourceContainer    = PostgresTestBase.createContainer()
  protected lazy val sourceDb: Database = sourceContainer.slickDatabase(SlickProfile.backend)

  override def beforeAll(): Unit = sourceContainer.start()
  override def afterAll(): Unit  = sourceContainer.stop()

  type FixtureParam = Database

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val targetContainer = PostgresTestBase.createEmptyContainer()
    targetContainer.start()
    val targetDb        = targetContainer.slickDatabase(SlickProfile.backend)

    complete {
      withFixture(test.toNoArgAsyncTest(targetDb))
    }.lastly {
      try targetDb.close()
      finally targetContainer.stop()
    }
  }

  def usersCopier(
      targetDb: Database,
      tableSpec: TableSpec = TableSpec.select(row => Seq(row.id, row.first_name, row.last_name, row.email)),
      limit: Option[Int] = None
  ) =
    TableCopier(
      sourceDb = sourceDb,
      targetDb = targetDb,
      schema = schema,
      tableName = "users",
      tableSpec = tableSpec,
      limit = limit
    )

  def profilesCopier(targetDb: Database, tableSpec: TableSpec) =
    TableCopier(
      sourceDb = sourceDb,
      targetDb = targetDb,
      schema = schema,
      tableName = "profiles",
      tableSpec = tableSpec
    )

  describe("copyTable") {
    it("copies all rows from source to target") { targetDb =>
      for {
        count       <- usersCopier(targetDb).run
        _           <- assert(count === 10)
        targetCount <- targetDb.run(sql"SELECT COUNT(*) FROM users".as[Int].head)
        _           <- assert(targetCount === 10)
      } yield succeed
    }

    it("applies a TableSpec transformer") { targetDb =>
      for {
        _      <- usersCopier(
                    targetDb,
                    tableSpec = TableSpec.select { row =>
                      Seq(
                        row.id,
                        row.first_name.mapString(_.toUpperCase),
                        row.last_name.mapString(_ => "REDACTED"),
                        row.email.mapString(_ => "anon@example.com")
                      )
                    }
                  ).run
        result <- targetDb.run(
                    sql"SELECT first_name, last_name, email FROM users WHERE id = 1".as[(String, String, String)].head
                  )
        _      <- assert(result._1 === "JOHN")
        _      <- assert(result._2 === "REDACTED")
        _      <- assert(result._3 === "anon@example.com")
      } yield succeed
    }

    it("respects the limit parameter") { targetDb =>
      for {
        count       <- usersCopier(targetDb, limit = Some(3)).run
        _           <- assert(count === 3)
        targetCount <- targetDb.run(sql"SELECT COUNT(*) FROM users".as[Int].head)
        _           <- assert(targetCount === 3)
      } yield succeed
    }

    it("produces deterministic anonymization") { targetDb =>
      val tableSpec = TableSpec.select { row =>
        Seq(
          row.id,
          row.first_name.mapString(Anonymizer.FirstName),
          row.last_name.mapString(Anonymizer.LastName),
          row.email.mapString(Anonymizer.Email)
        )
      }

      for {
        count                <- usersCopier(targetDb, tableSpec = tableSpec).run
        _                    <- assert(count === 10)
        (firstName1, email1) <- targetDb.run(sql"SELECT first_name, email FROM users WHERE id = 1".as[(String, String)].head)
        _                    <- assert(firstName1 != "John")
        _                    <- assert(email1.contains("@"))
        _                    <- assert(!email1.contains("john.doe"))
      } yield succeed
    }

    it("transforms JSONB columns") { targetDb =>
      for {
        _      <- usersCopier(targetDb).run
        count  <- profilesCopier(
                    targetDb,
                    tableSpec = TableSpec.select { row =>
                      Seq(row.id, row.user_id, row.phones.mapJsonArray(_.number.mapString(_ => "XXX-XXXX")), row.settings)
                    }
                  ).run
        _      <- assert(count === 8)
        phones <- targetDb.run(sql"SELECT phones FROM profiles WHERE user_id = 1".as[String].head)
        _      <- assert(phones.contains("XXX-XXXX"))
        _      <- assert(!phones.contains("555-0101"))
        _      <- assert(!phones.contains("555-0102"))
      } yield succeed
    }

    it("anonymizes phone numbers in JSONB with Anonymizer.PhoneNumber") { targetDb =>
      for {
        _      <- usersCopier(targetDb).run
        count  <- profilesCopier(
                    targetDb,
                    tableSpec = TableSpec.select { row =>
                      Seq(row.id, row.user_id, row.phones.mapJsonArray(_.number.mapString(Anonymizer.PhoneNumber)), row.settings)
                    }
                  ).run
        _      <- assert(count === 8)
        phones <- targetDb.run(sql"SELECT phones FROM profiles WHERE user_id = 1".as[String].head)
        _      <- assert(!phones.contains("555-0101"))
        _      <- assert(!phones.contains("555-0102"))
        _      <- assert(phones.contains("("))
      } yield succeed
    }

    describe("copies self-referencing tables") {
      it("with single parent FK") { targetDb =>
        for {
          count      <- TableCopier(
                          sourceDb = sourceDb,
                          targetDb = targetDb,
                          schema = schema,
                          tableName = "categories",
                          tableSpec = TableSpec.select(row => Seq(row.id, row.name, row.parent_id)),
                          batchSize = 3
                        ).run
          _          <- assert(count === 10)
          childCount <- targetDb.run(sql"SELECT COUNT(*) FROM categories WHERE parent_id IS NOT NULL".as[Int].head)
          _          <- assert(childCount === 7)
        } yield succeed
      }

      it("with multiple FKs") { targetDb =>
        for {
          count         <- TableCopier(
                             sourceDb = sourceDb,
                             targetDb = targetDb,
                             schema = schema,
                             tableName = "employees",
                             tableSpec = TableSpec.select(row => Seq(row.id, row.name, row.manager_id, row.mentor_id)),
                             batchSize = 2
                           ).run
          _             <- assert(count === 6)
          managedCount  <- targetDb.run(sql"SELECT COUNT(*) FROM employees WHERE manager_id IS NOT NULL".as[Int].head)
          _             <- assert(managedCount === 3)
          mentoredCount <- targetDb.run(sql"SELECT COUNT(*) FROM employees WHERE mentor_id IS NOT NULL".as[Int].head)
          _             <- assert(mentoredCount === 3)
        } yield succeed
      }

      it("with composite FKs") { targetDb =>
        for {
          count      <- TableCopier(
                          sourceDb = sourceDb,
                          targetDb = targetDb,
                          schema = schema,
                          tableName = "tree_nodes",
                          tableSpec = TableSpec.select { row =>
                            Seq(row.group_id, row.position, row.label, row.parent_group_id, row.parent_position)
                          },
                          batchSize = 2
                        ).run
          _          <- assert(count === 6)
          childCount <- targetDb.run(sql"SELECT COUNT(*) FROM tree_nodes WHERE parent_group_id IS NOT NULL".as[Int].head)
          _          <- assert(childCount === 4)
        } yield succeed
      }
    }

    it("preserves FK relationships") { targetDb =>
      for {
        _         <- usersCopier(targetDb).run
        count     <- TableCopier(
                       sourceDb = sourceDb,
                       targetDb = targetDb,
                       schema = schema,
                       tableName = "orders",
                       tableSpec = TableSpec.select(row => Seq(row.id, row.user_id, row.total, row.status))
                     ).run
        _         <- assert(count === 12)
        joinCount <- targetDb.run(
                       sql"""SELECT COUNT(*) FROM orders o
                JOIN users u ON o.user_id = u.id""".as[Int].head
                     )
        _         <- assert(joinCount === 12)
      } yield succeed
    }
  }

  describe("identifier quoting") {
    it("safely handles tables and columns with special characters") { targetDb =>
      val maliciousTableName  = "users; DROP TABLE orders; --"
      val maliciousColumnName = "data; DELETE FROM users; --"
      val createTableSql      =
        sqlu"""CREATE TABLE IF NOT EXISTS "#$maliciousTableName" (
          id SERIAL PRIMARY KEY,
          "#$maliciousColumnName" VARCHAR(100)
        )"""

      for {
        _ <- sourceDb.run(createTableSql)
        _ <- sourceDb.run(
               sqlu"""INSERT INTO "#$maliciousTableName" ("#$maliciousColumnName") VALUES ('test data 1'), ('test data 2')"""
             )
        _ <- targetDb.run(createTableSql)

        orderCountBefore <- sourceDb.run(sql"SELECT COUNT(*) FROM orders".as[Int].head)
        _                <- assert(orderCountBefore === 12, "Orders should exist before copy")

        count <- TableCopier(
                   sourceDb = sourceDb,
                   targetDb = targetDb,
                   schema = schema,
                   tableName = maliciousTableName,
                   tableSpec = TableSpec(
                     Seq(OutputColumn.SourceColumn("id"), OutputColumn.SourceColumn(maliciousColumnName)),
                     None
                   )
                 ).run
        _     <- assert(count === 2, "Should copy 2 rows from malicious table")

        orderCountAfter <- sourceDb.run(sql"SELECT COUNT(*) FROM orders".as[Int].head)
        _               <- assert(orderCountAfter === 12, "Orders table should not be affected by copying malicious-named table")

        targetCount <- targetDb.run(sql"""SELECT COUNT(*) FROM "#$maliciousTableName"""".as[Int].head)
        _           <- assert(targetCount === 2, "Target should have 2 rows")
      } yield succeed
    }
  }
}
