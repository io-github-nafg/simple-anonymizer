package simpleanonymizer

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AsyncFunSuite
import org.testcontainers.utility.MountableFile
import slick.additions.testcontainers.SlickPostgresContainer

import SlickProfile.api._

object PostgresTestBase {

  /** Create a PostgreSQL container with schema and test data loaded. */
  def createContainer(): SlickPostgresContainer = {
    val c = new SlickPostgresContainer()
    c.withCopyFileToContainer(
      MountableFile.forClasspathResource("01-schema.sql"),
      "/docker-entrypoint-initdb.d/01-schema.sql"
    )
    c.withCopyFileToContainer(
      MountableFile.forClasspathResource("02-data.sql"),
      "/docker-entrypoint-initdb.d/02-data.sql"
    )
    c
  }

  /** Create a PostgreSQL container with schema only (no data). */
  def createEmptyContainer(): SlickPostgresContainer = {
    val c = new SlickPostgresContainer()
    c.withCopyFileToContainer(
      MountableFile.forClasspathResource("01-schema.sql"),
      "/docker-entrypoint-initdb.d/01-schema.sql"
    )
    c
  }
}

/** Base trait for integration tests that need a PostgreSQL container with schema and test data. */
trait PostgresTestBase extends AsyncFunSuite with BeforeAndAfterAll {
  protected val dbMetadata: DbMetadata = new DbMetadata("public")

  protected lazy val container: SlickPostgresContainer = PostgresTestBase.createContainer()
  protected lazy val db: Database                      = container.slickDatabase(SlickProfile.backend)

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()
}
