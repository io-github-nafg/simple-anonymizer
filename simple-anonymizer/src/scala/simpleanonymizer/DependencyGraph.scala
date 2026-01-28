package simpleanonymizer

import scala.collection.mutable

/** FK dependency analysis for topological table ordering. */
object DependencyGraph {
  // Use ForeignKey from DbMetadata
  type ForeignKey = DbMetadata.ForeignKey

  /** Compute insertion levels for tables based on FK dependencies. Level 0 = tables with no FK dependencies (can be inserted first) Level N = tables that
    * depend only on tables at level < N
    *
    * Tables at the same level can be inserted in parallel. Returns Map[tableName -> level]
    */
  def computeTableLevels(tables: Seq[String], fks: Seq[ForeignKey]): Map[String, Int] = {
    // Build dependency map: table -> set of tables it depends on (parents)
    val dependencies: Map[String, Set[String]] = {
      val deps = mutable.Map[String, mutable.Set[String]]()
      for (table <- tables)
        deps(table) = mutable.Set.empty
      for (fk <- fks if fk.childTable != fk.parentTable) // Ignore self-references
        deps.get(fk.childTable).foreach(_ += fk.parentTable)
      deps.view.mapValues(_.toSet).toMap
    }

    val levels = mutable.Map[String, Int]()
    var changed = true

    // Initialize: tables with no dependencies are level 0
    for (table <- tables if dependencies(table).isEmpty)
      levels(table) = 0

    // Iterate until stable
    while (changed) {
      changed = false
      for (table <- tables if !levels.contains(table)) {
        val deps = dependencies(table)
        if (deps.forall(levels.contains)) {
          levels(table) = deps.map(levels).max + 1
          changed = true
        }
      }
    }

    // Check for cycles (tables not assigned a level)
    val unassigned = tables.filterNot(levels.contains)
    if (unassigned.nonEmpty)
      println(s"[DependencyGraph] WARNING: Circular dependencies detected for tables: ${unassigned.mkString(", ")}")

    levels.toMap
  }

  /** Group tables by their level */
  def groupTablesByLevel(tableLevels: Map[String, Int]): Seq[Seq[String]] = {
    if (tableLevels.isEmpty) return Seq.empty
    val maxLevel = tableLevels.values.max
    (0 to maxLevel).map { level =>
      tableLevels.collect { case (table, l) if l == level => table }.toSeq.sorted
    }
  }
}
