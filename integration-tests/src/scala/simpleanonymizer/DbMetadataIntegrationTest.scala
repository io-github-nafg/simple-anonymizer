package simpleanonymizer

class DbMetadataIntegrationTest extends PostgresTestBase {

  // ============================================================================
  // getForeignKeys tests
  // ============================================================================

  test("getForeignKeys returns all FK relationships") {
    // Should find: orders->users, order_items->orders, categories->categories, profiles->users
    for {
      fks <- db.run(dbMetadata.getAllForeignKeys)
    } yield {
      assert(fks.exists(fk => fk.fkTable.name == "orders" && fk.pkTable.name == "users"))
      assert(fks.exists(fk => fk.fkTable.name == "order_items" && fk.pkTable.name == "orders"))
      assert(fks.exists(fk => fk.fkTable.name == "categories" && fk.pkTable.name == "categories"))
      assert(fks.exists(fk => fk.fkTable.name == "profiles" && fk.pkTable.name == "users"))
    }
  }

  // ============================================================================
  // getAllTables tests
  // ============================================================================

  test("getAllTables returns all tables in schema") {
    for {
      tables <- db.run(dbMetadata.getAllTables)
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
