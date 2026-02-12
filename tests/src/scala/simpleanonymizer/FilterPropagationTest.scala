package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funspec.AnyFunSpec
import slick.jdbc.meta.MQName

class FilterPropagationTest extends AnyFunSpec with TypeCheckedTripleEquals {

  private def mqName(name: String): MQName = MQName(None, None, name)

  private def fk(childTable: String, childColumn: String, parentTable: String, parentColumn: String): DbContext.LogicalFK =
    DbContext.LogicalFK(None, mqName(childTable), mqName(parentTable), Seq(childColumn -> parentColumn))

  describe("computePropagatedFilters") {
    it("does not include explicit filters in the output") {
      val tables  = Seq("users")
      val filters = FilterPropagation.computePropagatedFilters(tables, Seq.empty) {
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
      assert(filters("orders").sql === """"user_id" IN (SELECT "id" FROM "users" WHERE active = true)""")
      assert(filters("order_items").sql.contains(""""order_id" IN (SELECT "id" FROM "orders" WHERE"""))
    }

    it("omits tables with no filter") {
      val tables  = Seq("users", "categories")
      val filters = FilterPropagation.computePropagatedFilters(tables, Seq.empty) {
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
      assert(clauses.exists(_.contains(""""order_id" IN (SELECT "id" FROM "orders" WHERE status = 'active')""")))
      assert(clauses.exists(_.contains(""""product_id" IN (SELECT "id" FROM "products" WHERE available = true)""")))
    }

    it("ANDs multiple parent clauses in the propagated subquery") {
      val tables  = Seq("users", "orders")
      val fks     = Seq(fk("orders", "user_id", "users", "id"))
      val filters =
        FilterPropagation.computePropagatedFilters(tables, fks) {
          case "users" => Some(TableSpec.WhereClause.Multiple("active = true", Seq("role = 'admin'")))
          case _       => None
        }

      assert(filters("orders").sql === """"user_id" IN (SELECT "id" FROM "users" WHERE (active = true) AND (role = 'admin'))""")
    }

    it("computes multiple self-ref CTEs independently from the same base filter") {
      val tables   = Seq("employees")
      val empTable = mqName("employees")
      val fks      = Seq(
        DbContext.LogicalFK(Some("fk_manager"), empTable, empTable, Seq("manager_id" -> "id")),
        DbContext.LogicalFK(Some("fk_mentor"), empTable, empTable, Seq("mentor_id" -> "id"))
      )
      val filters  =
        FilterPropagation.computePropagatedFilters(tables, fks) {
          case "employees" => Some(TableSpec.WhereClause.Single("active = true"))
          case _           => None
        }

      val clauses = filters("employees").clauses
      assert(clauses.size === 2)
      // Each CTE should use the same base filter (active = true), not embed the other CTE
      clauses.foreach { clause =>
        assert(clause.contains("WITH RECURSIVE"), s"Each clause should be a recursive CTE: $clause")
        assert(clause.contains("active = true"), s"Each clause should reference the base filter: $clause")
      }
      // Neither CTE should contain a nested WITH RECURSIVE
      clauses.foreach { clause =>
        val cteCount = "WITH RECURSIVE".r.findAllIn(clause).size
        assert(cteCount === 1, s"Should not nest CTEs (found $cteCount): $clause")
      }
    }
  }
}
