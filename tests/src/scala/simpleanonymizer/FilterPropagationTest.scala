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

  describe("computePropagatedFilters") {
    it("does not include explicit filters in the output") {
      val tables  = Seq("users")
      val fks     = Seq.empty[MForeignKey]
      val filters = FilterPropagation.computePropagatedFilters(tables, fks) {
        case "users" => Some(TableSpec.WhereClause.Single("active = true"))
        case _       => None
      }
      assert(!filters.contains("users"))
    }

    it("propagates filters through FK chains") {
      val tables  = Seq("users", "orders", "order_items")
      val fks     = Seq(
        fk("orders", "user_id", "users", "id"),
        fk("order_items", "order_id", "orders", "id")
      )
      val filters =
        FilterPropagation.computePropagatedFilters(tables, fks) {
          case "users" => Some(TableSpec.WhereClause.Single("active = true"))
          case _       => None
        }

      assert(!filters.contains("users"))
      assert(filters("orders").sql === "user_id IN (SELECT id FROM users WHERE active = true)")
      assert(filters("order_items").sql.contains("order_id IN (SELECT id FROM orders WHERE"))
    }

    it("omits tables with no filter") {
      val tables  = Seq("users", "categories")
      val fks     = Seq.empty[MForeignKey]
      val filters = FilterPropagation.computePropagatedFilters(tables, fks) {
        case "users" => Some(TableSpec.WhereClause.Single("active = true"))
        case _       => None
      }
      assert(!filters.contains("users"))
      assert(!filters.contains("categories"))
    }

    it("propagates from multiple filtered parents to a shared child") {
      val tables  = Seq("orders", "products", "order_items")
      val fks     = Seq(
        fk("order_items", "order_id", "orders", "id"),
        fk("order_items", "product_id", "products", "id")
      )
      val filters =
        FilterPropagation.computePropagatedFilters(tables, fks) {
          case "orders"   => Some(TableSpec.WhereClause.Single("status = 'active'"))
          case "products" => Some(TableSpec.WhereClause.Single("available = true"))
          case _          => None
        }

      val clauses = filters("order_items").clauses
      assert(clauses.size === 2)
      assert(clauses.exists(_.contains("order_id IN (SELECT id FROM orders WHERE status = 'active')")))
      assert(clauses.exists(_.contains("product_id IN (SELECT id FROM products WHERE available = true)")))
    }

    it("ANDs multiple parent clauses in the propagated subquery") {
      val tables  = Seq("users", "orders")
      val fks     = Seq(fk("orders", "user_id", "users", "id"))
      val filters =
        FilterPropagation.computePropagatedFilters(tables, fks) {
          case "users" => Some(TableSpec.WhereClause.Multiple("active = true", Seq("role = 'admin'")))
          case _       => None
        }

      assert(filters("orders").sql === "user_id IN (SELECT id FROM users WHERE (active = true) AND (role = 'admin'))")
    }
  }
}
