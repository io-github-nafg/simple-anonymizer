package simpleanonymizer

class CoverageValidatorIntegrationTest extends PostgresTestBase {

  private def getTableMQName(tableName: String) =
    db.run(dbMetadata.getAllTables).map(_.find(_.name.name == tableName).get.name)

  private lazy val fkColumnsByTableFut =
    db.run(dbMetadata.getAllForeignKeys).map(CoverageValidator.fkColumnsByTable)

  describe("getDataColumns") {
    it("returns non-PK, non-FK columns") {
      for {
        mqname         <- getTableMQName("users")
        fkColumnsByTbl <- fkColumnsByTableFut
        cols           <- db.run(CoverageValidator.getDataColumns(mqname, fkColumnsByTbl))
      } yield {
        assert(cols.contains("first_name"))
        assert(cols.contains("last_name"))
        assert(cols.contains("email"))
        assert(!cols.contains("id"))
      }
    }

    it("excludes FK columns") {
      for {
        mqname         <- getTableMQName("profiles")
        fkColumnsByTbl <- fkColumnsByTableFut
        cols           <- db.run(CoverageValidator.getDataColumns(mqname, fkColumnsByTbl))
      } yield {
        assert(!cols.contains("user_id"))
        assert(cols.contains("phones"))
      }
    }
  }
}
