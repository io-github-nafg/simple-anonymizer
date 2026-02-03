package simpleanonymizer

import scala.concurrent.Future

class CoverageValidatorIntegrationTest extends PostgresTestBase {
  private lazy val validatorFut: Future[CoverageValidator] =
    for {
      fks       <- db.run(dbMetadata.getAllForeignKeys)
      validator <- db.run(CoverageValidator(dbMetadata, fks))
    } yield validator

  describe("getDataColumns") {
    it("returns non-PK, non-FK columns") {
      for {
        validator <- validatorFut
      } yield {
        val cols = validator.getDataColumns("users")
        assert(cols.contains("first_name"))
        assert(cols.contains("last_name"))
        assert(cols.contains("email"))
        assert(!cols.contains("id"))
      }
    }

    it("excludes FK columns") {
      for {
        validator <- validatorFut
      } yield {
        val cols = validator.getDataColumns("profiles")
        assert(!cols.contains("user_id"))
        assert(cols.contains("phones"))
      }
    }
  }
}
