package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funspec.AnyFunSpec
import slick.jdbc.meta.{MForeignKey, MQName}
import slick.model.ForeignKeyAction

class FilterPropagationTest extends AnyFunSpec with TypeCheckedTripleEquals {

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

  describe("generateChildWhereClause") {
    it("returns None when no parent has a filter") {
      val fks           = Seq(fk("orders", "user_id", "users", "id"))
      val parentFilters = Map.empty[String, String]
      val result        = FilterPropagation.generateChildWhereClause("orders", parentFilters, fks)
      assert(result === None)
    }

    it("generates a subquery for a single FK") {
      val fks           = Seq(fk("orders", "user_id", "users", "id"))
      val parentFilters = Map("users" -> "created_at > '2024-01-01'")
      val result        = FilterPropagation.generateChildWhereClause("orders", parentFilters, fks)
      assert(result === Some("user_id IN (SELECT id FROM users WHERE created_at > '2024-01-01')"))
    }

    it("combines multiple FKs with AND") {
      val fks           = Seq(
        fk("order_items", "order_id", "orders", "id"),
        fk("order_items", "product_id", "products", "id")
      )
      val parentFilters = Map(
        "orders"   -> "status = 'active'",
        "products" -> "available = true"
      )
      val result        = FilterPropagation.generateChildWhereClause("order_items", parentFilters, fks)
      assert(result.isDefined)
      val clause        = result.get
      assert(clause.contains("order_id IN (SELECT id FROM orders WHERE status = 'active')"))
      assert(clause.contains("product_id IN (SELECT id FROM products WHERE available = true)"))
      assert(clause.contains(" AND "))
    }
  }

  describe("computeEffectiveFilters") {
    it("uses an explicit whereClause") {
      val tables  = Seq("users")
      val fks     = Seq.empty[MForeignKey]
      val specs   = Map("users" -> TableSpec.select(row => Seq(row.name)).where("active = true"))
      val filters = FilterPropagation.computeEffectiveFilters(tables, fks, specs)
      assert(filters("users") === Some("active = true"))
    }

    it("propagates filters through FK chains") {
      val tables  = Seq("users", "orders", "order_items")
      val fks     = Seq(
        fk("orders", "user_id", "users", "id"),
        fk("order_items", "order_id", "orders", "id")
      )
      val specs   = Map("users" -> TableSpec.select(row => Seq(row.name)).where("active = true"))
      val filters = FilterPropagation.computeEffectiveFilters(tables, fks, specs)

      assert(filters("users") === Some("active = true"))
      assert(filters("orders") === Some("user_id IN (SELECT id FROM users WHERE active = true)"))
      assert(filters("order_items").isDefined)
      assert(filters("order_items").get.contains("order_id IN (SELECT id FROM orders WHERE"))
    }
  }
}
