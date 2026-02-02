package simpleanonymizer

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.funspec.AnyFunSpec
import slick.jdbc.meta.{MForeignKey, MQName}
import slick.model.ForeignKeyAction

class TableSorterTest extends AnyFunSpec with TypeCheckedTripleEquals {

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

  describe("computeTableLevels") {
    it("assigns level 0 to tables with no FK dependencies") {
      val tables = Seq("users", "products", "categories")
      val fks    = Seq.empty[MForeignKey]
      val levels = TableSorter.computeTableLevels(tables, fks)
      assert(levels === Map("users" -> 0, "products" -> 0, "categories" -> 0))
    }

    it("assigns higher levels based on dependencies") {
      val tables = Seq("users", "orders")
      val fks    = Seq(fk("orders", "user_id", "users", "id"))
      val levels = TableSorter.computeTableLevels(tables, fks)
      assert(levels("users") === 0)
      assert(levels("orders") === 1)
    }

    it("handles diamond dependency pattern") {
      val tables = Seq("users", "orders", "products", "order_items")
      val fks    = Seq(
        fk("orders", "user_id", "users", "id"),
        fk("products", "user_id", "users", "id"),
        fk("order_items", "order_id", "orders", "id"),
        fk("order_items", "product_id", "products", "id")
      )
      val levels = TableSorter.computeTableLevels(tables, fks)
      assert(levels("users") === 0)
      assert(levels("orders") === 1)
      assert(levels("products") === 1)
      assert(levels("order_items") === 2)
    }

    it("ignores self-referencing FKs") {
      val tables = Seq("categories")
      val fks    = Seq(fk("categories", "parent_id", "categories", "id"))
      val levels = TableSorter.computeTableLevels(tables, fks)
      assert(levels("categories") === 0)
    }

    it("handles circular dependencies gracefully") {
      val tables = Seq("a", "b")
      val fks    = Seq(
        fk("a", "b_id", "b", "id"),
        fk("b", "a_id", "a", "id")
      )
      val levels = TableSorter.computeTableLevels(tables, fks)
      assert(levels.isEmpty || (!levels.contains("a") && !levels.contains("b")))
    }

    it("handles empty inputs") {
      assert(TableSorter.computeTableLevels(Seq.empty, Seq.empty).isEmpty)
    }
  }

  describe("groupTablesByLevel") {
    it("groups and sorts correctly") {
      val levels = Map("c" -> 0, "a" -> 0, "b" -> 1, "d" -> 2)
      val groups = TableSorter.groupTablesByLevel(levels)
      assert(groups.length === 3)
      assert(groups(0) === Seq("a", "c"))
      assert(groups(1) === Seq("b"))
      assert(groups(2) === Seq("d"))
    }

    it("handles empty map") {
      assert(TableSorter.groupTablesByLevel(Map.empty) === Seq.empty)
    }
  }
}
