package simpleanonymizer

import java.sql.{Connection, DriverManager}

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.containers.PostgreSQLContainer

class DbSnapshotIntegrationTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  import DbSnapshot._
  import RowTransformer.DSL._

  private var sourceContainer: PostgreSQLContainer[_] = _
  private var targetContainer: PostgreSQLContainer[_] = _
  private var sourceConn: Connection = _
  private var targetConn: Connection = _

  override def beforeAll(): Unit = {
    sourceContainer = new PostgreSQLContainer("postgres:15")
    targetContainer = new PostgreSQLContainer("postgres:15")
    sourceContainer.start()
    targetContainer.start()
  }

  override def afterAll(): Unit = {
    if (sourceConn != null) sourceConn.close()
    if (targetConn != null) targetConn.close()
    if (sourceContainer != null) sourceContainer.stop()
    if (targetContainer != null) targetContainer.stop()
  }

  override def beforeEach(): Unit = {
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

    setupTestSchema(sourceConn)
    setupTestSchema(targetConn)

    // Clean up tables before each test (copyTable commits, so rollback won't work)
    cleanupTables(sourceConn)
    cleanupTables(targetConn)
  }

  private def cleanupTables(conn: Connection): Unit = {
    val stmt = conn.createStatement()
    stmt.execute("TRUNCATE profiles, order_items, orders, categories, users CASCADE")
    conn.commit()
    stmt.close()
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
  }

  private def setupTestSchema(conn: Connection): Unit = {
    val stmt = conn.createStatement()

    // Create test tables with FK relationships
    stmt.execute("""
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email VARCHAR(200)
      )
    """)

    stmt.execute("""
      CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id),
        total DECIMAL(10,2),
        status VARCHAR(50)
      )
    """)

    stmt.execute("""
      CREATE TABLE IF NOT EXISTS order_items (
        id SERIAL PRIMARY KEY,
        order_id INTEGER REFERENCES orders(id),
        product_name VARCHAR(200),
        quantity INTEGER
      )
    """)

    // Table with self-reference
    stmt.execute("""
      CREATE TABLE IF NOT EXISTS categories (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        parent_id INTEGER REFERENCES categories(id)
      )
    """)

    // Table with JSONB column
    stmt.execute("""
      CREATE TABLE IF NOT EXISTS profiles (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id),
        phones JSONB,
        settings JSONB
      )
    """)

    conn.commit()
    stmt.close()
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
  // copyTable tests
  // ============================================================================

  test("copyTable copies data between databases") {
    // Insert test data in source
    val insertStmt = sourceConn.prepareStatement(
      "INSERT INTO users (first_name, last_name, email) VALUES (?, ?, ?)"
    )
    insertStmt.setString(1, "John")
    insertStmt.setString(2, "Doe")
    insertStmt.setString(3, "john@example.com")
    insertStmt.executeUpdate()
    insertStmt.setString(1, "Jane")
    insertStmt.setString(2, "Smith")
    insertStmt.setString(3, "jane@example.com")
    insertStmt.executeUpdate()
    sourceConn.commit()
    insertStmt.close()

    // Copy to target
    val columns = getTableColumns(sourceConn, "users")
    val count = copyTable(
      sourceConn = sourceConn,
      targetConn = targetConn,
      tableName = "users",
      columns = columns
    )

    assert(count === 2)

    // Verify data was copied to target
    val selectStmt = targetConn.createStatement()
    val rs = selectStmt.executeQuery("SELECT COUNT(*) FROM users")
    rs.next()
    assert(rs.getInt(1) === 2)
    rs.close()
    selectStmt.close()
  }

  test("copyTable applies transformer") {
    // Insert test data in source
    val insertStmt = sourceConn.prepareStatement(
      "INSERT INTO users (first_name, last_name, email) VALUES (?, ?, ?)"
    )
    insertStmt.setString(1, "John")
    insertStmt.setString(2, "Doe")
    insertStmt.setString(3, "john@example.com")
    insertStmt.executeUpdate()
    sourceConn.commit()
    insertStmt.close()

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
    val rs = selectStmt.executeQuery("SELECT first_name, last_name, email FROM users")
    rs.next()
    assert(rs.getString("first_name") === "JOHN")
    assert(rs.getString("last_name") === "REDACTED")
    assert(rs.getString("email") === "anon@example.com")
    rs.close()
    selectStmt.close()
  }

  test("copyTable handles JSONB columns with transformer") {
    // Insert test data in source
    val insertStmt = sourceConn.prepareStatement(
      "INSERT INTO users (first_name, last_name, email) VALUES (?, ?, ?) RETURNING id"
    )
    insertStmt.setString(1, "John")
    insertStmt.setString(2, "Doe")
    insertStmt.setString(3, "john@example.com")
    val userRs = insertStmt.executeQuery()
    userRs.next()
    val userId = userRs.getInt(1)
    userRs.close()
    insertStmt.close()

    val profileStmt = sourceConn.prepareStatement(
      "INSERT INTO profiles (user_id, phones, settings) VALUES (?, ?::jsonb, ?::jsonb)"
    )
    profileStmt.setInt(1, userId)
    profileStmt.setString(2, """[{"type":"home","number":"555-1234"},{"type":"work","number":"555-5678"}]""")
    profileStmt.setString(3, """{"theme":"dark"}""")
    profileStmt.executeUpdate()
    sourceConn.commit()
    profileStmt.close()

    // First copy users to target (needed for FK)
    val userColumns = getTableColumns(sourceConn, "users")
    copyTable(
      sourceConn = sourceConn,
      targetConn = targetConn,
      tableName = "users",
      columns = userColumns
    )

    val transformer = table(
      "phones" -> jsonArray("number")(_ => "XXX-XXXX")
    )

    val columns = getTableColumns(sourceConn, "profiles")
    copyTable(
      sourceConn = sourceConn,
      targetConn = targetConn,
      tableName = "profiles",
      columns = columns,
      transformer = Some(transformer)
    )

    // Verify JSONB transformation in target
    val selectStmt = targetConn.createStatement()
    val rs = selectStmt.executeQuery("SELECT phones FROM profiles")
    rs.next()
    val phones = rs.getString("phones")
    assert(phones.contains("XXX-XXXX"))
    assert(!phones.contains("555-1234"))
    assert(!phones.contains("555-5678"))
    rs.close()
    selectStmt.close()
  }

  // ============================================================================
  // truncateTable tests
  // ============================================================================

  test("truncateTable removes all rows") {
    // Insert test data
    val insertStmt = sourceConn.prepareStatement(
      "INSERT INTO categories (name) VALUES (?)"
    )
    insertStmt.setString(1, "Test1")
    insertStmt.executeUpdate()
    insertStmt.setString(1, "Test2")
    insertStmt.executeUpdate()
    sourceConn.commit()
    insertStmt.close()

    // Verify data exists
    val countStmt = sourceConn.createStatement()
    val rs1 = countStmt.executeQuery("SELECT COUNT(*) FROM categories")
    rs1.next()
    assert(rs1.getInt(1) === 2)
    rs1.close()

    // Truncate
    truncateTable(sourceConn, "categories")

    // Verify empty
    val rs2 = countStmt.executeQuery("SELECT COUNT(*) FROM categories")
    rs2.next()
    assert(rs2.getInt(1) === 0)
    rs2.close()
    countStmt.close()
  }
}
