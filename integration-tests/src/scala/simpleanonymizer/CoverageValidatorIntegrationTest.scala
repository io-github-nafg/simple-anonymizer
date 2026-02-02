package simpleanonymizer

class CoverageValidatorIntegrationTest extends PostgresTestBase {

  // Helper to get MQName from the database tables
  private def getTableMQName(tableName: String) =
    db.run(dbMetadata.getAllTables).map(_.find(_.name.name == tableName).get.name)

  // ============================================================================
  // getDataColumns tests
  // ============================================================================

  test("getDataColumns returns non-PK, non-FK columns") {
    for {
      mqname <- getTableMQName("users")
      cols   <- db.run(CoverageValidator.getDataColumns(mqname))
    } yield {
      assert(cols.contains("first_name"))
      assert(cols.contains("last_name"))
      assert(cols.contains("email"))
      assert(!cols.contains("id")) // PK excluded
    }
  }

  test("getDataColumns excludes FK columns") {
    for {
      mqname <- getTableMQName("profiles")
      cols   <- db.run(CoverageValidator.getDataColumns(mqname))
    } yield {
      assert(!cols.contains("user_id")) // FK excluded
      assert(cols.contains("phones"))   // data column included
    }
  }
}
