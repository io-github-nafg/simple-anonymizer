package simpleanonymizer

class DbMetadataIntegrationTest extends PostgresTestBase {

  describe("getForeignKeys") {
    it("returns all FK relationships") {
      for {
        fks <- dbMetadata.allForeignKeys
      } yield {
        assert(fks.exists(fk => fk.fkTable.name == "orders" && fk.pkTable.name == "users"))
        assert(fks.exists(fk => fk.fkTable.name == "order_items" && fk.pkTable.name == "orders"))
        assert(fks.exists(fk => fk.fkTable.name == "categories" && fk.pkTable.name == "categories"))
        assert(fks.exists(fk => fk.fkTable.name == "profiles" && fk.pkTable.name == "users"))
      }
    }
  }

  describe("getAllTables") {
    it("returns all tables in the schema") {
      for {
        tables <- dbMetadata.allTables
      } yield {
        val tableNames = tables.map(_.name.name)
        assert(tableNames.contains("users"))
        assert(tableNames.contains("orders"))
        assert(tableNames.contains("order_items"))
        assert(tableNames.contains("categories"))
        assert(tableNames.contains("profiles"))
      }
    }
  }
}
