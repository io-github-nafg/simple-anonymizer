package simpleanonymizer

import org.scalatest.{BeforeAndAfterAll, FutureOutcome}
import org.scalatest.funsuite.FixtureAsyncFunSuite
import org.testcontainers.utility.MountableFile
import slick.additions.testcontainers.SlickPostgresContainer
import SlickProfile.api._

class DbSnapshotIntegrationTest extends FixtureAsyncFunSuite with BeforeAndAfterAll {
  import DbSnapshot._
  import RowTransformer.DSL._

  // Source container: loaded with schema AND data via init scripts (shared across all tests)
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

  override def beforeAll(): Unit = {
    sourceContainer.start()
  }

  override def afterAll(): Unit = {
    sourceContainer.stop()
  }

  // Fixture: fresh target database for each test
  type FixtureParam = Database

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val targetContainer = new SlickPostgresContainer()
    targetContainer.withCopyFileToContainer(
      MountableFile.forClasspathResource("01-schema.sql"),
      "/docker-entrypoint-initdb.d/01-schema.sql"
    )
    targetContainer.start()
    val targetDb = targetContainer.slickDatabase(SlickProfile.backend)

    complete {
      withFixture(test.toNoArgAsyncTest(targetDb))
    }.lastly {
      try targetDb.close()
      finally targetContainer.stop()
    }
  }

  // ============================================================================
  // getForeignKeys tests
  // ============================================================================

  test("getForeignKeys returns all FK relationships") { _ =>
    // Should find: orders->users, order_items->orders, categories->categories, profiles->users
    for {
      fks <- sourceDb.run(getForeignKeys())
      _ <- assert(fks.exists(fk => fk.fkTable.name == "orders" && fk.pkTable.name == "users"))
      _ <- assert(fks.exists(fk => fk.fkTable.name == "order_items" && fk.pkTable.name == "orders"))
      _ <- assert(fks.exists(fk => fk.fkTable.name == "categories" && fk.pkTable.name == "categories"))
      _ <- assert(fks.exists(fk => fk.fkTable.name == "profiles" && fk.pkTable.name == "users"))
    } yield succeed
  }

  // ============================================================================
  // getAllTables tests
  // ============================================================================

  test("getAllTables returns all tables in schema") { _ =>
    for {
      tables <- sourceDb.run(getAllTables())
      _ <- assert(tables.contains("users"))
      _ <- assert(tables.contains("orders"))
      _ <- assert(tables.contains("order_items"))
      _ <- assert(tables.contains("categories"))
      _ <- assert(tables.contains("profiles"))
    } yield succeed
  }

  // ============================================================================
  // getPrimaryKeyColumns tests
  // ============================================================================

  test("getPrimaryKeyColumns returns PK columns") { _ =>
    for {
      pkCols <- sourceDb.run(getPrimaryKeyColumns("users"))
      _ <- assert(pkCols === Set("id"))
    } yield succeed
  }

  // ============================================================================
  // getForeignKeyColumns tests
  // ============================================================================

  test("getForeignKeyColumns returns FK columns") { _ =>
    for {
      fkCols <- sourceDb.run(getForeignKeyColumns("orders"))
      _ <- assert(fkCols === Set("user_id"))
      orderItemsFkCols <- sourceDb.run(getForeignKeyColumns("order_items"))
      _ <- assert(orderItemsFkCols === Set("order_id"))
    } yield succeed
  }

  // ============================================================================
  // getTableColumns tests
  // ============================================================================

  test("getTableColumns returns all columns") { _ =>
    for {
      cols <- sourceDb.run(getTableColumns("users"))
      _ <- assert(cols.toSet === Set("id", "first_name", "last_name", "email"))
    } yield succeed
  }

  // ============================================================================
  // validateTransformerCoverage tests
  // ============================================================================

  test("validateTransformerCoverage succeeds when all non-PK/FK columns covered") { _ =>
    val transformer = table(
      "first_name" -> passthrough,
      "last_name" -> passthrough,
      "email" -> passthrough
    )
    for {
      result <- sourceDb.run(validateTransformerCoverage("users", transformer))
      _ <- assert(result === Right(()))
    } yield succeed
  }

  test("validateTransformerCoverage fails when columns missing") { _ =>
    val transformer = table(
      "first_name" -> passthrough
      // missing last_name and email
    )
    for {
      result <- sourceDb.run(validateTransformerCoverage("users", transformer))
      _ <- assert(result.isLeft)
      _ <- assert(result.left.getOrElse(Set.empty).contains("last_name"))
      _ <- assert(result.left.getOrElse(Set.empty).contains("email"))
    } yield succeed
  }

  // ============================================================================
  // copyTable tests - using pre-populated source data
  // ============================================================================

  test("copyTable copies all users from source to target") { targetDb =>
    for {
      columns <- sourceDb.run(getTableColumns("users"))
      count <- copyTable(
        sourceDb = sourceDb,
        targetDb = targetDb,
        tableName = "users",
        columns = columns
      )
      _ <- assert(count === 10) // 10 users in 02-data.sql
      targetCount <- targetDb.run(sql"SELECT COUNT(*) FROM users".as[Int].head)
      _ <- assert(targetCount === 10)
    } yield succeed
  }

  test("copyTable applies transformer") { targetDb =>
    val transformer = table(
      "first_name" -> using(_.toUpperCase),
      "last_name" -> using(_ => "REDACTED"),
      "email" -> using(_ => "anon@example.com")
    )

    for {
      columns <- sourceDb.run(getTableColumns("users"))
      _ <- copyTable(
        sourceDb = sourceDb,
        targetDb = targetDb,
        tableName = "users",
        columns = columns,
        transformer = Some(transformer)
      )
      result <- targetDb.run(
        sql"SELECT first_name, last_name, email FROM users WHERE id = 1".as[(String, String, String)].head
      )
      _ <- assert(result._1 === "JOHN") // John -> JOHN
      _ <- assert(result._2 === "REDACTED")
      _ <- assert(result._3 === "anon@example.com")
    } yield succeed
  }

  test("copyTable with limit copies only requested number of rows") { targetDb =>
    for {
      columns <- sourceDb.run(getTableColumns("users"))
      count <- copyTable(
        sourceDb = sourceDb,
        targetDb = targetDb,
        tableName = "users",
        columns = columns,
        limit = Some(3)
      )
      _ <- assert(count === 3)
      targetCount <- targetDb.run(sql"SELECT COUNT(*) FROM users".as[Int].head)
      _ <- assert(targetCount === 3)
    } yield succeed
  }

  test("copyTable copies users with deterministic anonymization") { targetDb =>
    import DeterministicAnonymizer._

    val transformer = table(
      "first_name" -> using(FirstName.anonymize),
      "last_name" -> using(LastName.anonymize),
      "email" -> using(Email.anonymize)
    )

    for {
      columns <- sourceDb.run(getTableColumns("users"))
      count <- copyTable(
        sourceDb = sourceDb,
        targetDb = targetDb,
        tableName = "users",
        columns = columns,
        transformer = Some(transformer)
      )
      _ <- assert(count === 10)
      (firstName1, email1) <- targetDb.run(
        sql"SELECT first_name, email FROM users WHERE id = 1".as[(String, String)].head
      )
      _ <- assert(firstName1 != "John") // Should be anonymized
      _ <- assert(email1.contains("@")) // Should still be valid email format
      _ <- assert(!email1.contains("john.doe")) // Should not contain original
    } yield succeed
  }

  test("copyTable handles JSONB columns with transformer") { targetDb =>
    val transformer = table(
      "phones" -> jsonArray("number")(_ => "XXX-XXXX")
    )

    for {
      // First copy users to target (needed for FK)
      userColumns <- sourceDb.run(getTableColumns("users"))
      _ <- copyTable(sourceDb, targetDb, "users", userColumns)
      // Now copy profiles with transformation
      columns <- sourceDb.run(getTableColumns("profiles"))
      count <- copyTable(
        sourceDb = sourceDb,
        targetDb = targetDb,
        tableName = "profiles",
        columns = columns,
        transformer = Some(transformer)
      )
      _ <- assert(count === 8) // 8 profiles in 02-data.sql
      phones <- targetDb.run(sql"SELECT phones FROM profiles WHERE user_id = 1".as[String].head)
      _ <- assert(phones.contains("XXX-XXXX"))
      _ <- assert(!phones.contains("555-0101"))
      _ <- assert(!phones.contains("555-0102"))
    } yield succeed
  }

  test("copyTable copies profiles with JSONB phone anonymization") { targetDb =>
    import DeterministicAnonymizer._

    val transformer = table(
      "phones" -> jsonArray("number")(PhoneNumber.anonymize)
    )

    for {
      // First copy users (needed for FK constraint)
      userColumns <- sourceDb.run(getTableColumns("users"))
      _ <- copyTable(sourceDb, targetDb, "users", userColumns)
      // Now copy profiles with transformation
      columns <- sourceDb.run(getTableColumns("profiles"))
      count <- copyTable(
        sourceDb = sourceDb,
        targetDb = targetDb,
        tableName = "profiles",
        columns = columns,
        transformer = Some(transformer)
      )
      _ <- assert(count === 8)
      phones <- targetDb.run(sql"SELECT phones FROM profiles WHERE user_id = 1".as[String].head)
      _ <- assert(!phones.contains("555-0101"))
      _ <- assert(!phones.contains("555-0102"))
      _ <- assert(phones.contains("(")) // Should have phone format (XXX) XXX-XXXX
    } yield succeed
  }

  test("copyTable copies hierarchical categories") { targetDb =>
    for {
      columns <- sourceDb.run(getTableColumns("categories"))
      count <- copyTable(
        sourceDb = sourceDb,
        targetDb = targetDb,
        tableName = "categories",
        columns = columns
      )
      _ <- assert(count === 10) // 3 root + 7 children in 02-data.sql
      childCount <- targetDb.run(sql"SELECT COUNT(*) FROM categories WHERE parent_id IS NOT NULL".as[Int].head)
      _ <- assert(childCount === 7) // 7 child categories
    } yield succeed
  }

  test("copyTable copies orders with FK relationships") { targetDb =>
    for {
      // First copy users (needed for FK)
      userColumns <- sourceDb.run(getTableColumns("users"))
      _ <- copyTable(sourceDb, targetDb, "users", userColumns)
      // Now copy orders
      columns <- sourceDb.run(getTableColumns("orders"))
      count <- copyTable(sourceDb, targetDb, "orders", columns)
      _ <- assert(count === 12) // 12 orders in 02-data.sql
      joinCount <- targetDb.run(
        sql"""SELECT COUNT(*) FROM orders o
              JOIN users u ON o.user_id = u.id""".as[Int].head
      )
      _ <- assert(joinCount === 12) // All orders should join with users
    } yield succeed
  }
}
