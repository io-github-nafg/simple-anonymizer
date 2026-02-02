package simpleanonymizer

import org.scalatest.funsuite.FixtureAsyncFunSuite
import org.scalatest.{BeforeAndAfterAll, FutureOutcome}
import simpleanonymizer.SlickProfile.api._
import slick.jdbc.meta.{MColumn, MQName}

import scala.concurrent.Future

class TableCopierIntegrationTest extends FixtureAsyncFunSuite with BeforeAndAfterAll {

  protected val schema: String = "public"

  def table(name: String) = MQName(None, Some(schema), name)

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

  def getTableColumns(name: String): Future[Vector[String]] = sourceDb.run(MColumn.getColumns(table(name), "%")).map(_.map(_.name))

  // ============================================================================
  // copyTable tests - using pre-populated source data
  // ============================================================================

  test("copyTable copies all users from source to target") { targetDb =>
    for {
      columns     <- getTableColumns("users")
      count       <- TableCopier.copyTable(
                       sourceDb = sourceDb,
                       targetDb = targetDb,
                       schema = schema,
                       tableName = "users",
                       columns = columns
                     )
      _           <- assert(count === 10) // 10 users in 02-data.sql
      targetCount <- targetDb.run(sql"SELECT COUNT(*) FROM users".as[Int].head)
      _           <- assert(targetCount === 10)
    } yield succeed
  }

  test("copyTable applies transformer") { targetDb =>
    val tableSpec = TableSpec.select { row =>
      Seq(
        row.first_name.mapString(_.toUpperCase),
        row.last_name.mapString(_ => "REDACTED"),
        row.email.mapString(_ => "anon@example.com")
      )
    }

    for {
      columns <- getTableColumns("users")
      _       <- TableCopier.copyTable(
                   sourceDb = sourceDb,
                   targetDb = targetDb,
                   schema = schema,
                   tableName = "users",
                   columns = columns,
                   tableSpec = Some(tableSpec)
                 )
      result  <- targetDb.run(
                   sql"SELECT first_name, last_name, email FROM users WHERE id = 1".as[(String, String, String)].head
                 )
      _       <- assert(result._1 === "JOHN") // John -> JOHN
      _       <- assert(result._2 === "REDACTED")
      _       <- assert(result._3 === "anon@example.com")
    } yield succeed
  }

  test("copyTable with limit copies only requested number of rows") { targetDb =>
    for {
      columns     <- getTableColumns("users")
      count       <- TableCopier.copyTable(
                       sourceDb = sourceDb,
                       targetDb = targetDb,
                       schema = schema,
                       tableName = "users",
                       columns = columns,
                       limit = Some(3)
                     )
      _           <- assert(count === 3)
      targetCount <- targetDb.run(sql"SELECT COUNT(*) FROM users".as[Int].head)
      _           <- assert(targetCount === 3)
    } yield succeed
  }

  test("copyTable copies users with deterministic anonymization") { targetDb =>
    val transformer = TableSpec.select { row =>
      Seq(
        row.first_name.mapString(Anonymizer.FirstName),
        row.last_name.mapString(Anonymizer.LastName),
        row.email.mapString(Anonymizer.Email)
      )
    }

    for {
      columns              <- getTableColumns("users")
      count                <- TableCopier.copyTable(
                                sourceDb = sourceDb,
                                targetDb = targetDb,
                                schema = schema,
                                tableName = "users",
                                columns = columns,
                                tableSpec = Some(transformer)
                              )
      _                    <- assert(count === 10)
      (firstName1, email1) <- targetDb.run(
                                sql"SELECT first_name, email FROM users WHERE id = 1".as[(String, String)].head
                              )
      _                    <- assert(firstName1 != "John")         // Should be anonymized
      _                    <- assert(email1.contains("@"))         // Should still be a valid email format
      _                    <- assert(!email1.contains("john.doe")) // Should not contain original
    } yield succeed
  }

  test("copyTable handles JSONB columns with transformer") { targetDb =>
    val transformer = TableSpec.select { row =>
      Seq(row.phones.mapJsonArray(_.number.mapString(_ => "XXX-XXXX")))
    }

    for {
      // First, copy users to the target (needed for FK)
      userColumns <- getTableColumns("users")
      _           <- TableCopier.copyTable(sourceDb, targetDb, schema, "users", userColumns)
      // Now copy profiles with transformation
      columns     <- getTableColumns("profiles")
      count       <- TableCopier.copyTable(
                       sourceDb = sourceDb,
                       targetDb = targetDb,
                       schema = schema,
                       tableName = "profiles",
                       columns = columns,
                       tableSpec = Some(transformer)
                     )
      _           <- assert(count === 8) // 8 profiles in 02-data.sql
      phones      <- targetDb.run(sql"SELECT phones FROM profiles WHERE user_id = 1".as[String].head)
      _           <- assert(phones.contains("XXX-XXXX"))
      _           <- assert(!phones.contains("555-0101"))
      _           <- assert(!phones.contains("555-0102"))
    } yield succeed
  }

  test("copyTable copies profiles with JSONB phone anonymization") { targetDb =>
    val transformer = TableSpec.select { row =>
      Seq(row.phones.mapJsonArray(_.number.mapString(Anonymizer.PhoneNumber)))
    }

    for {
      // First copy users (needed for FK constraint)
      userColumns <- getTableColumns("users")
      _           <- TableCopier.copyTable(sourceDb, targetDb, schema, "users", userColumns)
      // Now copy profiles with transformation
      columns     <- getTableColumns("profiles")
      count       <- TableCopier.copyTable(
                       sourceDb = sourceDb,
                       targetDb = targetDb,
                       schema = schema,
                       tableName = "profiles",
                       columns = columns,
                       tableSpec = Some(transformer)
                     )
      _           <- assert(count === 8)
      phones      <- targetDb.run(sql"SELECT phones FROM profiles WHERE user_id = 1".as[String].head)
      _           <- assert(!phones.contains("555-0101"))
      _           <- assert(!phones.contains("555-0102"))
      _           <- assert(phones.contains("(")) // Should have a phone format (XXX) XXX-XXXX
    } yield succeed
  }

  test("copyTable copies hierarchical categories") { targetDb =>
    for {
      columns    <- getTableColumns("categories")
      count      <- TableCopier.copyTable(
                      sourceDb = sourceDb,
                      targetDb = targetDb,
                      schema = schema,
                      tableName = "categories",
                      columns = columns
                    )
      _          <- assert(count === 10)     // 3 roots + 7 children in 02-data.sql
      childCount <- targetDb.run(sql"SELECT COUNT(*) FROM categories WHERE parent_id IS NOT NULL".as[Int].head)
      _          <- assert(childCount === 7) // 7 child categories
    } yield succeed
  }

  test("copyTable copies orders with FK relationships") { targetDb =>
    for {
      // First copy users (needed for FK)
      userColumns <- getTableColumns("users")
      _           <- TableCopier.copyTable(sourceDb, targetDb, schema, "users", userColumns)
      // Now copy orders
      columns     <- getTableColumns("orders")
      count       <- TableCopier.copyTable(sourceDb, targetDb, schema, "orders", columns)
      _           <- assert(count === 12)     // 12 orders in 02-data.sql
      joinCount   <- targetDb.run(
                       sql"""SELECT COUNT(*) FROM orders o
              JOIN users u ON o.user_id = u.id""".as[Int].head
                     )
      _           <- assert(joinCount === 12) // All orders should join with users
    } yield succeed
  }

  // ============================================================================
  // Identifier quoting tests - verify that unusual table/column names are handled safely
  // ============================================================================

  test("copyTable handles table with special characters in name") { targetDb =>
    // Create a table with a malicious name in both source and target
    val maliciousTableName  = "users; DROP TABLE orders; --"
    val maliciousColumnName = "data; DELETE FROM users; --"
    val createTableSql      =
      sqlu"""CREATE TABLE IF NOT EXISTS "#$maliciousTableName" (
        id SERIAL PRIMARY KEY,
        "#$maliciousColumnName" VARCHAR(100)
      )"""

    for {
      // Create the malicious table in source and target
      _ <- sourceDb.run(createTableSql)
      _ <- sourceDb.run(
             sqlu"""INSERT INTO "#$maliciousTableName" ("#$maliciousColumnName") VALUES ('test data 1'), ('test data 2')"""
           )
      _ <- targetDb.run(createTableSql)

      // Verify orders table exists before the copy
      orderCountBefore <- sourceDb.run(sql"SELECT COUNT(*) FROM orders".as[Int].head)
      _                <- assert(orderCountBefore === 12, "Orders should exist before copy")

      // Copy the malicious table - quoteIdentifier should make this safe
      columns <- getTableColumns(maliciousTableName)
      count   <- TableCopier.copyTable(sourceDb, targetDb, schema, maliciousTableName, columns)
      _       <- assert(count === 2, "Should copy 2 rows from malicious table")

      // CRITICAL: Verify orders table still exists (wasn't dropped by injection)
      orderCountAfter <- sourceDb.run(sql"SELECT COUNT(*) FROM orders".as[Int].head)
      _               <- assert(orderCountAfter === 12, "Orders table should not be affected by copying malicious-named table")

      // Verify the data was actually copied correctly
      targetCount <- targetDb.run(sql"""SELECT COUNT(*) FROM "#$maliciousTableName"""".as[Int].head)
      _           <- assert(targetCount === 2, "Target should have 2 rows")
    } yield succeed
  }
}
