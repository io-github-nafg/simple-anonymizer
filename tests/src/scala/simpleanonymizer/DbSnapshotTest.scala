package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funsuite.AnyFunSuite
import slick.jdbc.meta.{MForeignKey, MQName}
import slick.model.ForeignKeyAction

class DbSnapshotTest extends AnyFunSuite with TypeCheckedTripleEquals {
  import DbSnapshot._

  // Helper to create MForeignKey for tests (fkTable = child, pkTable = parent)
  private def fk(childTable: String, childColumn: String, parentTable: String, parentColumn: String): MForeignKey =
    MForeignKey(
      pkTable = MQName(None, None, parentTable),
      pkColumn = parentColumn,
      fkTable = MQName(None, None, childTable),
      fkColumn = childColumn,
      keySeq = 1,
      updateRule = ForeignKeyAction.NoAction,
      deleteRule = ForeignKeyAction.NoAction,
      fkName = None,
      pkName = None,
      deferrability = 0
    )

  // ============================================================================
  // computeTableLevels - FK graph algorithm (core logic, no DB needed)
  // ============================================================================

  test("computeTableLevels assigns level 0 to tables with no FK dependencies") {
    val tables = Seq("users", "products", "categories")
    val fks = Seq.empty[MForeignKey]
    val levels = computeTableLevels(tables, fks)
    assert(levels === Map("users" -> 0, "products" -> 0, "categories" -> 0))
  }

  test("computeTableLevels assigns higher levels based on dependencies") {
    // orders depends on users (FK: orders.user_id -> users.id)
    val tables = Seq("users", "orders")
    val fks = Seq(fk("orders", "user_id", "users", "id"))
    val levels = computeTableLevels(tables, fks)
    assert(levels("users") === 0)
    assert(levels("orders") === 1)
  }

  test("computeTableLevels handles diamond dependency pattern") {
    // Diamond: A <- B, A <- C, B <- D, C <- D
    // users <- orders, users <- products, orders <- order_items, products <- order_items
    val tables = Seq("users", "orders", "products", "order_items")
    val fks = Seq(
      fk("orders", "user_id", "users", "id"),
      fk("products", "user_id", "users", "id"),
      fk("order_items", "order_id", "orders", "id"),
      fk("order_items", "product_id", "products", "id")
    )
    val levels = computeTableLevels(tables, fks)
    assert(levels("users") === 0)
    assert(levels("orders") === 1)
    assert(levels("products") === 1)
    assert(levels("order_items") === 2)
  }

  test("computeTableLevels ignores self-referencing FKs") {
    // categories with parent_id -> categories.id (self-reference)
    val tables = Seq("categories")
    val fks = Seq(fk("categories", "parent_id", "categories", "id"))
    val levels = computeTableLevels(tables, fks)
    assert(levels("categories") === 0)
  }

  test("computeTableLevels handles circular dependencies gracefully") {
    // A -> B -> A (circular)
    val tables = Seq("a", "b")
    val fks = Seq(
      fk("a", "b_id", "b", "id"),
      fk("b", "a_id", "a", "id")
    )
    val levels = computeTableLevels(tables, fks)
    // Neither should be assigned a level due to circular dependency
    assert(levels.isEmpty || (!levels.contains("a") && !levels.contains("b")))
  }

  test("computeTableLevels handles empty inputs") {
    assert(computeTableLevels(Seq.empty, Seq.empty).isEmpty)
  }

  // ============================================================================
  // groupTablesByLevel
  // ============================================================================

  test("groupTablesByLevel groups and sorts correctly") {
    val levels = Map("c" -> 0, "a" -> 0, "b" -> 1, "d" -> 2)
    val groups = groupTablesByLevel(levels)
    assert(groups.length === 3)
    assert(groups(0) === Seq("a", "c")) // Sorted within level
    assert(groups(1) === Seq("b"))
    assert(groups(2) === Seq("d"))
  }

  test("groupTablesByLevel handles empty map") {
    assert(groupTablesByLevel(Map.empty) === Seq.empty)
  }

  // ============================================================================
  // generateChildWhereClause - filter propagation logic
  // ============================================================================

  test("generateChildWhereClause returns None when no parent has filter") {
    val fks = Seq(fk("orders", "user_id", "users", "id"))
    val parentFilters = Map.empty[String, String]
    val result = generateChildWhereClause("orders", parentFilters, fks)
    assert(result === None)
  }

  test("generateChildWhereClause generates subquery for single FK") {
    val fks = Seq(fk("orders", "user_id", "users", "id"))
    val parentFilters = Map("users" -> "created_at > '2024-01-01'")
    val result = generateChildWhereClause("orders", parentFilters, fks)
    assert(result === Some("user_id IN (SELECT id FROM users WHERE created_at > '2024-01-01')"))
  }

  test("generateChildWhereClause combines multiple FKs with AND") {
    val fks = Seq(
      fk("order_items", "order_id", "orders", "id"),
      fk("order_items", "product_id", "products", "id")
    )
    val parentFilters = Map(
      "orders" -> "status = 'active'",
      "products" -> "available = true"
    )
    val result = generateChildWhereClause("order_items", parentFilters, fks)
    assert(result.isDefined)
    val clause = result.get
    assert(clause.contains("order_id IN (SELECT id FROM orders WHERE status = 'active')"))
    assert(clause.contains("product_id IN (SELECT id FROM products WHERE available = true)"))
    assert(clause.contains(" AND "))
  }

  // ============================================================================
  // computeEffectiveFilters - filter propagation through graph
  // ============================================================================

  test("computeEffectiveFilters uses explicit whereClause") {
    val tables = Seq("users")
    val fks = Seq.empty[MForeignKey]
    val configs = Map("users" -> TableConfig(whereClause = Some("active = true")))
    val filters = computeEffectiveFilters(tables, fks, configs)
    assert(filters("users") === Some("active = true"))
  }

  test("computeEffectiveFilters returns None for skip and copyAll") {
    val tables = Seq("audit_log", "config")
    val fks = Seq.empty[MForeignKey]
    val configs = Map(
      "audit_log" -> TableConfig(skip = true),
      "config" -> TableConfig(copyAll = true)
    )
    val filters = computeEffectiveFilters(tables, fks, configs)
    assert(filters("audit_log") === None)
    assert(filters("config") === None)
  }

  test("computeEffectiveFilters propagates filters through FK chain") {
    val tables = Seq("users", "orders", "order_items")
    val fks = Seq(
      fk("orders", "user_id", "users", "id"),
      fk("order_items", "order_id", "orders", "id")
    )
    val configs = Map("users" -> TableConfig(whereClause = Some("active = true")))
    val filters = computeEffectiveFilters(tables, fks, configs)

    assert(filters("users") === Some("active = true"))
    assert(filters("orders") === Some("user_id IN (SELECT id FROM users WHERE active = true)"))
    // order_items gets filter propagated from orders
    assert(filters("order_items").isDefined)
    assert(filters("order_items").get.contains("order_id IN (SELECT id FROM orders WHERE"))
  }

  // ============================================================================
  // TableConfig
  // ============================================================================

  test("TableConfig has sensible defaults") {
    val config = TableConfig()
    assert(config.whereClause === None)
    assert(config.skip === false)
    assert(config.copyAll === false)
  }
}
