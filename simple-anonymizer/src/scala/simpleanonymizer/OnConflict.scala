package simpleanonymizer

/** Specifies behavior when an INSERT conflicts with existing data.
  *
  * Used in `TableSpec` to configure upsert behavior for target tables that may already contain rows.
  */
case class OnConflict(target: OnConflict.ConflictTarget, action: OnConflict.Action)
object OnConflict {
  sealed trait Action
  object Action {

    /** ON CONFLICT DO NOTHING - skip conflicting rows without error. */
    case object DoNothing extends Action

    /** ON CONFLICT DO UPDATE - update conflicting rows with new values.
      *
      * @param updateColumns
      *   Columns to update on conflict. If None, updates all columns except the conflict target columns.
      */
    case class DoUpdate(updateColumns: Option[Set[String]] = None) extends Action
  }

  /** Target for conflict detection in ON CONFLICT clause. */
  sealed trait ConflictTarget
  object ConflictTarget {

    /** Use primary key columns (auto-detected from database metadata). */
    case object PrimaryKey extends ConflictTarget

    /** Explicit columns for conflict detection. */
    case class Columns(columns: Seq[String]) extends ConflictTarget

    /** Named unique constraint for conflict detection. */
    case class Constraint(name: String) extends ConflictTarget
  }

  // noinspection MutatorLikeMethodIsParameterless
  def doNothing                                                        = OnConflict(ConflictTarget.PrimaryKey, Action.DoNothing)
  def doNothing(targetColumns: String*)                                = OnConflict(ConflictTarget.Columns(targetColumns), Action.DoNothing)
  // noinspection MutatorLikeMethodIsParameterless
  def doUpdate                                                         = OnConflict(ConflictTarget.PrimaryKey, Action.DoUpdate())
  def doUpdate(targetColumns: String*)                                 = OnConflict(ConflictTarget.Columns(targetColumns), Action.DoUpdate())
  def doUpdate(targetColumns: Seq[String], updateColumns: Set[String]) = OnConflict(ConflictTarget.Columns(targetColumns), Action.DoUpdate(Some(updateColumns)))
}
