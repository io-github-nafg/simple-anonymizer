package simpleanonymizer

class CoverageValidatorIntegrationTest extends PostgresTestBase {
  private lazy val validator: CoverageValidator = CoverageValidator(dbMetadata)

  describe("getDataColumns") {
    it("returns non-PK, non-FK columns") {
      for {
        cols <- validator.getDataColumns("users")
      } yield {
        assert(cols.contains("first_name"))
        assert(cols.contains("last_name"))
        assert(cols.contains("email"))
        assert(!cols.contains("id"))
      }
    }

    it("excludes FK columns") {
      for {
        cols <- validator.getDataColumns("profiles")
      } yield {
        assert(!cols.contains("user_id"))
        assert(cols.contains("phones"))
      }
    }
  }
}
