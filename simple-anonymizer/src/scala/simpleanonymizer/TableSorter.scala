package simpleanonymizer

import slick.jdbc.meta.{MForeignKey, MTable}

import scala.annotation.tailrec

/** Computes table insertion order based on foreign key dependencies.
  *
  * Tables are grouped into levels where:
  *   - Level 0: Tables with no FK dependencies (can be inserted first)
  *   - Level N: Tables that depend only on tables at levels < N
  *
  * This ensures parent tables are populated before child tables that reference them.
  */
object TableSorter {

  /** Compute insertion levels for tables based on FK dependencies.
    *
    * @param tables
    *   All tables to process
    * @param fks
    *   Foreign key relationships between tables
    * @return
    *   Map of table name to level number. Tables not in the map have circular dependencies.
    */
  private[simpleanonymizer] def computeTableLevels(tables: Seq[String], fks: Seq[MForeignKey]): Map[String, Int] = {
    val tableSet = tables.toSet

    // Build dependency map: table -> set of tables it depends on (parents)
    // Only include tables that are in our input list and ignore self-references
    val dependencies: Map[String, Set[String]] =
      tables.map { table =>
        val deps = fks
          .filter(fk => fk.fkTable.name == table && fk.pkTable.name != table && tableSet.contains(fk.pkTable.name))
          .map(_.pkTable.name)
          .toSet
        table -> deps
      }.toMap

    @tailrec
    def assignLevels(levels: Map[String, Int]): Map[String, Int] = {
      val unassigned = tables.filterNot(levels.contains)
      if (unassigned.isEmpty) levels
      else {
        val newlyAssigned = unassigned.flatMap { table =>
          val deps = dependencies(table)
          if (deps.isEmpty)
            Some(table -> 0)
          else if (deps.forall(levels.contains))
            Some(table -> (deps.map(levels).max + 1))
          else
            None
        }
        if (newlyAssigned.isEmpty) {
          // Circular dependency detected - no progress possible
          println(
            s"[TableSorter] WARNING: Circular dependencies detected for tables: ${unassigned.mkString(", ")}. " +
              "These tables will not be copied."
          )
          levels
        } else
          assignLevels(levels ++ newlyAssigned)
      }
    }

    assignLevels(Map.empty)
  }

  /** Group tables by their level for parallel processing.
    *
    * @param tableLevels
    *   Map of table name to level number
    * @return
    *   Tables grouped by level, sorted within each level. Tables at the same level can be copied in parallel.
    */
  private[simpleanonymizer] def groupTablesByLevel(tableLevels: Map[String, Int]): Seq[Seq[String]] =
    if (tableLevels.isEmpty)
      Seq.empty
    else
      (0 to tableLevels.values.max).map { level =>
        tableLevels.collect { case (table, `level`) => table }.toSeq.sorted
      }

  /** Compute table insertion order based on FK dependencies.
    *
    * @param tables
    *   All tables to process
    * @param fks
    *   Foreign key relationships between tables
    * @return
    *   Tables grouped by level. Level 0 tables have no dependencies and should be inserted first. Tables at the same level can be inserted in parallel.
    */
  def apply(tables: Seq[MTable], fks: Seq[MForeignKey]): Seq[Seq[MTable]] = {
    val tableMap    = tables.map(t => t.name.name -> t).toMap
    val tableNames  = tables.map(_.name.name)
    val tableLevels = computeTableLevels(tableNames, fks)
    groupTablesByLevel(tableLevels).map(_.flatMap(tableMap.get))
  }
}
