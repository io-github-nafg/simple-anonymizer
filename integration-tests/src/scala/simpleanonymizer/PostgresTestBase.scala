package simpleanonymizer

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AsyncFunSpec
import org.testcontainers.utility.MountableFile
import slick.additions.testcontainers.SlickPostgresContainer

import SlickProfile.api._

object PostgresTestBase {

  private def newContainer(withData: Boolean): SlickPostgresContainer = {
    val c = new SlickPostgresContainer()
    c.withCopyFileToContainer(
      MountableFile.forClasspathResource("01-schema.sql"),
      "/docker-entrypoint-initdb.d/01-schema.sql"
    )
    if (withData)
      c.withCopyFileToContainer(
        MountableFile.forClasspathResource("02-data.sql"),
        "/docker-entrypoint-initdb.d/02-data.sql"
      )
    c
  }

  /** Shared source container (schema + data), started once per JVM. */
  lazy val sourceContainer: SlickPostgresContainer = {
    val c = newContainer(withData = true)
    c.start()
    sys.addShutdownHook(c.stop())
    c
  }

  /** Shared target container (schema only, used as template), started once per JVM. */
  lazy val targetContainer: SlickPostgresContainer = {
    val c = newContainer(withData = false)
    c.start()
    sys.addShutdownHook(c.stop())
    c
  }

  private val dbCounter = new java.util.concurrent.atomic.AtomicInteger(0)

  private def adminUrl(container: SlickPostgresContainer): String = {
    val host = container.getHost
    val port = container.getMappedPort(5432)
    s"jdbc:postgresql://$host:$port/postgres"
  }

  private def dbUrl(container: SlickPostgresContainer, dbName: String): String = {
    val host = container.getHost
    val port = container.getMappedPort(5432)
    s"jdbc:postgresql://$host:$port/$dbName"
  }

  /** Create a fresh database in the target container from the template. Returns (Database, dbName). */
  def createTargetDb(): (Database, String) = {
    val tc         = targetContainer
    val dbName     = s"test_${dbCounter.incrementAndGet()}"
    val templateDb = tc.getDatabaseName
    val conn       = java.sql.DriverManager.getConnection(adminUrl(tc), tc.getUsername, tc.getPassword)
    try
      conn
        .createStatement()
        .execute(
          s"""CREATE DATABASE "$dbName" TEMPLATE "$templateDb""""
        )
    finally conn.close()
    val db         = Database.forURL(dbUrl(tc, dbName), tc.getUsername, tc.getPassword, driver = "org.postgresql.Driver")
    (db, dbName)
  }

  /** Drop a database in the target container. */
  def dropTargetDb(dbName: String): Unit = {
    val tc   = targetContainer
    val conn = java.sql.DriverManager.getConnection(adminUrl(tc), tc.getUsername, tc.getPassword)
    try conn.createStatement().execute(s"""DROP DATABASE IF EXISTS "$dbName"""")
    finally conn.close()
  }
}

/** Base trait for integration tests that need a PostgreSQL container with schema and test data. */
trait PostgresTestBase extends AsyncFunSpec with BeforeAndAfterAll {
  protected val dbMetadata: DbMetadata = new DbMetadata("public")

  protected lazy val db: Database = PostgresTestBase.sourceContainer.slickDatabase(SlickProfile.backend)

  override def afterAll(): Unit = db.close()
}
