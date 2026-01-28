package simpleanonymizer

import java.sql.{Connection, DriverManager}

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.MountableFile

class DbSnapshotIntegrationTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  import DbSnapshot._
  import RowTransformer.DSL._

  // Source container: loaded with schema AND data via init scripts (shared across all tests)
  private var sourceContainer: PostgreSQLContainer[_] = _
  // Target container: schema only, fresh for each test
  private var targetContainer: PostgreSQLContainer[_] = _
  private var sourceConn: Connection = _
  private var targetConn: Connection = _

  private def createSourceContainer(): PostgreSQLContainer[_] = {
    val container = new PostgreSQLContainer("postgres:15")
    // Load schema first, then data
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

  private def createTargetContainer(): PostgreSQLContainer[_] = {
    val container = new PostgreSQLContainer("postgres:15")
    // Target only gets schema, no data
    container.withCopyFileToContainer(
      MountableFile.forClasspathResource("01-schema.sql"),
      "/docker-entrypoint-initdb.d/01-schema.sql"
    )
    container
  }

  override def beforeAll(): Unit = {
    sourceContainer = createSourceContainer()
    sourceContainer.start()
  }

  override def afterAll(): Unit = {
    if (sourceConn != null) sourceConn.close()
    if (targetConn != null) targetConn.close()
    if (sourceContainer != null) sourceContainer.stop()
    if (targetContainer != null) targetContainer.stop()
  }

  override def beforeEach(): Unit = {
    // Fresh target container for each test
    if (targetContainer != null) {
      targetContainer.stop()
    }
    targetContainer = createTargetContainer()
    targetContainer.start()

    sourceConn = DriverManager.getConnection(
      sourceContainer.getJdbcUrl,
      sourceContainer.getUsername,
      sourceContainer.getPassword
    )
    sourceConn.setAutoCommit(false)

    targetConn = DriverManager.getConnection(
      targetContainer.getJdbcUrl,
      targetContainer.getUsername,
      targetContainer.getPassword
    )
    targetConn.setAutoCommit(false)
  }

  override def afterEach(): Unit = {
    if (sourceConn != null && !sourceConn.isClosed) {
      sourceConn.rollback()
      sourceConn.close()
    }
    if (targetConn != null && !targetConn.isClosed) {
      targetConn.rollback()
      targetConn.close()
    }
    // Don't stop targetContainer here - it will be stopped in the next beforeEach
    // or in afterAll
  }

  // ============================================================================
  // getForeignKeys tests
  // ============================================================================

  test("getForeignKeys returns all FK relationships") {
    val fks = getForeignKeys(sourceConn)

    // Should find: orders->users, order_items->orders, categories->categories, profiles->users
    assert(fks.exists(fk => fk.childTable == "orders" && fk.parentTable == "users"))
    assert(fks.exists(fk => fk.childTable == "order_items" && fk.parentTable == "orders"))
    assert(fks.exists(fk => fk.childTable == "categories" && fk.parentTable == "categories"))
    assert(fks.exists(fk => fk.childTable == "profiles" && fk.parentTable == "users"))
  }

  // ============================================================================
  // getAllTables tests
  // ============================================================================

  test("getAllTables returns all tables in schema") {
    val tables = getAllTables(sourceConn)

    assert(tables.contains("users"))
    assert(tables.contains("orders"))
    assert(tables.contains("order_items"))
    assert(tables.contains("categories"))
    assert(tables.contains("profiles"))
  }

  // ============================================================================
  // getPrimaryKeyColumns tests
  // ============================================================================

  test("getPrimaryKeyColumns returns PK columns") {
    val pkCols = getPrimaryKeyColumns(sourceConn, "users")
    assert(pkCols === Set("id"))
  }

  // ============================================================================
  // getForeignKeyColumns tests
  // ============================================================================

  test("getForeignKeyColumns returns FK columns") {
    val fkCols = getForeignKeyColumns(sourceConn, "orders")
    assert(fkCols === Set("user_id"))

    val orderItemsFkCols = getForeignKeyColumns(sourceConn, "order_items")
    assert(orderItemsFkCols === Set("order_id"))
  }

  // ============================================================================
  // getTableColumns tests
  // ============================================================================

  test("getTableColumns returns all columns") {
    val cols = getTableColumns(sourceConn, "users")
    assert(cols.toSet === Set("id", "first_name", "last_name", "email"))
  }

  // ============================================================================
  // validateTransformerCoverage tests
  // ============================================================================

  test("validateTransformerCoverage succeeds when all non-PK/FK columns covered") {
    val transformer = table(
      "first_name" -> passthrough,
      "last_name" -> passthrough,
      "email" -> passthrough
    )
    val result = validateTransformerCoverage(sourceConn, "users", transformer)
    assert(result === Right(()))
  }

  test("validateTransformerCoverage fails when columns missing") {
    val transformer = table(
      "first_name" -> passthrough
      // missing last_name and email
    )
    val result = validateTransformerCoverage(sourceConn, "users", transformer)
    assert(result.isLeft)
    assert(result.left.getOrElse(Set.empty).contains("last_name"))
    assert(result.left.getOrElse(Set.empty).contains("email"))
  }

  // ============================================================================
  // copyTable tests - using pre-populated source data
  // ============================================================================

  test("copyTable copies all users from source to target") {
    val columns = getTableColumns(sourceConn, "users")
    val count = copyTable(
      sourceConn = sourceConn,
      targetConn = targetConn,
      tableName = "users",
      columns = columns
    )

    assert(count === 10) // 10 users in 02-data.sql

    // Verify data was copied to target
    val selectStmt = targetConn.createStatement()
    val rs = selectStmt.executeQuery("SELECT COUNT(*) FROM users")
    rs.next()
    assert(rs.getInt(1) === 10)
    rs.close()
    selectStmt.close()
  }

  test("copyTable applies transformer") {
    val transformer = table(
      "first_name" -> using(_.toUpperCase),
      "last_name" -> using(_ => "REDACTED"),
      "email" -> using(_ => "anon@example.com")
    )

    val columns = getTableColumns(sourceConn, "users")
    copyTable(
      sourceConn = sourceConn,
      targetConn = targetConn,
      tableName = "users",
      columns = columns,
      transformer = Some(transformer)
    )

    // Verify transformation was applied in target
    val selectStmt = targetConn.createStatement()
    val rs = selectStmt.executeQuery("SELECT first_name, last_name, email FROM users WHERE id = 1")
    rs.next()
    assert(rs.getString("first_name") === "JOHN") // John -> JOHN
    assert(rs.getString("last_name") === "REDACTED")
    assert(rs.getString("email") === "anon@example.com")
    rs.close()
    selectStmt.close()
  }

  test("copyTable with limit copies only requested number of rows") {
    val columns = getTableColumns(sourceConn, "users")
    val count = copyTable(
      sourceConn = sourceConn,
      targetConn = targetConn,
      tableName = "users",
      columns = columns,
      limit = Some(3)
    )

    assert(count === 3)

    val selectStmt = targetConn.createStatement()
    val rs = selectStmt.executeQuery("SELECT COUNT(*) FROM users")
    rs.next()
    assert(rs.getInt(1) === 3)
    rs.close()
    selectStmt.close()
  }

  test("copyTable copies users with deterministic anonymization") {
    import DeterministicAnonymizer._

    val transformer = table(
      "first_name" -> using(FirstName.anonymize),
      "last_name" -> using(LastName.anonymize),
      "email" -> using(Email.anonymize)
    )

    val columns = getTableColumns(sourceConn, "users")
    val count = copyTable(
      sourceConn = sourceConn,
      targetConn = targetConn,
      tableName = "users",
      columns = columns,
      transformer = Some(transformer)
    )

    assert(count === 10)

    // Verify deterministic anonymization
    val selectStmt = targetConn.createStatement()
    val rs = selectStmt.executeQuery("SELECT first_name, email FROM users WHERE id = 1")
    rs.next()
    val firstName1 = rs.getString("first_name")
    val email1 = rs.getString("email")
    assert(firstName1 != "John") // Should be anonymized
    assert(email1.contains("@")) // Should still be valid email format
    assert(!email1.contains("john.doe")) // Should not contain original
    rs.close()
    selectStmt.close()
  }

  test("copyTable handles JSONB columns with transformer") {
    // First copy users to target (needed for FK)
    val userColumns = getTableColumns(sourceConn, "users")
    copyTable(sourceConn, targetConn, "users", userColumns)

    val transformer = table(
      "phones" -> jsonArray("number")(_ => "XXX-XXXX")
    )

    val columns = getTableColumns(sourceConn, "profiles")
    val count = copyTable(
      sourceConn = sourceConn,
      targetConn = targetConn,
      tableName = "profiles",
      columns = columns,
      transformer = Some(transformer)
    )

    assert(count === 8) // 8 profiles in 02-data.sql

    // Verify JSONB transformation in target
    val selectStmt = targetConn.createStatement()
    val rs = selectStmt.executeQuery("SELECT phones FROM profiles WHERE user_id = 1")
    rs.next()
    val phones = rs.getString("phones")
    assert(phones.contains("XXX-XXXX"))
    assert(!phones.contains("555-0101"))
    assert(!phones.contains("555-0102"))
    rs.close()
    selectStmt.close()
  }

  test("copyTable copies profiles with JSONB phone anonymization") {
    // First copy users (needed for FK constraint)
    val userColumns = getTableColumns(sourceConn, "users")
    copyTable(sourceConn, targetConn, "users", userColumns)

    import DeterministicAnonymizer._

    val transformer = table(
      "phones" -> jsonArray("number")(PhoneNumber.anonymize)
    )

    val columns = getTableColumns(sourceConn, "profiles")
    val count = copyTable(
      sourceConn = sourceConn,
      targetConn = targetConn,
      tableName = "profiles",
      columns = columns,
      transformer = Some(transformer)
    )

    assert(count === 8)

    // Verify phone numbers were anonymized
    val selectStmt = targetConn.createStatement()
    val rs = selectStmt.executeQuery("SELECT phones FROM profiles WHERE user_id = 1")
    rs.next()
    val phones = rs.getString("phones")
    assert(!phones.contains("555-0101"))
    assert(!phones.contains("555-0102"))
    assert(phones.contains("(")) // Should have phone format (XXX) XXX-XXXX
    rs.close()
    selectStmt.close()
  }

  test("copyTable copies hierarchical categories") {
    val columns = getTableColumns(sourceConn, "categories")
    val count = copyTable(
      sourceConn = sourceConn,
      targetConn = targetConn,
      tableName = "categories",
      columns = columns
    )

    assert(count === 10) // 3 root + 7 children in 02-data.sql

    // Verify hierarchical structure was preserved
    val selectStmt = targetConn.createStatement()
    val rs = selectStmt.executeQuery(
      "SELECT COUNT(*) FROM categories WHERE parent_id IS NOT NULL"
    )
    rs.next()
    assert(rs.getInt(1) === 7) // 7 child categories
    rs.close()
    selectStmt.close()
  }

  test("copyTable copies orders with FK relationships") {
    // First copy users (needed for FK)
    val userColumns = getTableColumns(sourceConn, "users")
    copyTable(sourceConn, targetConn, "users", userColumns)

    val columns = getTableColumns(sourceConn, "orders")
    val count = copyTable(sourceConn, targetConn, "orders", columns)

    assert(count === 12) // 12 orders in 02-data.sql

    // Verify FK relationships are valid
    val selectStmt = targetConn.createStatement()
    val rs = selectStmt.executeQuery(
      """SELECT COUNT(*) FROM orders o
         JOIN users u ON o.user_id = u.id"""
    )
    rs.next()
    assert(rs.getInt(1) === 12) // All orders should join with users
    rs.close()
    selectStmt.close()
  }

}
