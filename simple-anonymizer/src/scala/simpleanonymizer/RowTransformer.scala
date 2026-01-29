package simpleanonymizer

import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._

/** Composable row transformation for table anonymization.
  *
  * Provides a DSL for specifying column transformations including:
  *   - Simple passthrough (preserves original database type)
  *   - Value transformation with DeterministicAnonymizer (String => String)
  *   - JSON navigation (fields within arrays)
  *   - Dependent columns (e.g., zip code based on state)
  *   - Fixed values and null
  */
object RowTransformer {

  private type Row = Map[String, String]

  // ============================================================================
  // Column Plan - single ADT for column transformation
  // ============================================================================

  /** Describes how to transform a column. This is the core internal representation used by DbSnapshot. */
  sealed trait ColumnPlan {
    def columnName: String
  }
  object ColumnPlan       {

    /** Passthrough - preserve the original database value and type */
    case class Passthrough(columnName: String) extends ColumnPlan

    /** Set to null */
    case class SetNull(columnName: String) extends ColumnPlan

    /** Fixed value of any type */
    case class Fixed(columnName: String, value: Any) extends ColumnPlan

    /** String transformation with optional JSON navigation */
    case class Transform(columnName: String, nav: JsonNav, f: String => String) extends ColumnPlan

    /** Dependent transformation - needs other column values */
    case class Dependent(columnName: String, f: Row => String => String) extends ColumnPlan
  }

  // ============================================================================
  // JSON Navigation
  // ============================================================================

  /** JsonNav works on JSON values internally to avoid repeated parse/encode cycles. */
  sealed trait JsonNav {

    /** Transform a Json value, applying f to leaf string values */
    def transformJson(json: Json, f: String => String): Json

    /** Wrap a String => String function to work on JSON strings */
    def wrap(f: String => String): String => String = { jsonStr =>
      val json        = parse(jsonStr).getOrElse(Json.Null)
      val transformed = transformJson(json, f)
      transformed.noSpaces
    }
  }
  object JsonNav {

    case object Direct extends JsonNav {
      def transformJson(json: Json, f: String => String): Json = {
        val str         = json.asString.getOrElse("")
        val transformed = f(str)
        Json.fromString(transformed)
      }

      // Override wrap for Direct to avoid an unnecessary JSON round-trip
      override def wrap(f: String => String): String => String = f
    }

    case class Field(fieldName: String) extends JsonNav {
      def transformJson(json: Json, f: String => String): Json =
        json.asObject match {
          case Some(obj) =>
            val oldValue = obj(fieldName).flatMap(_.asString).getOrElse("")
            val newValue = f(oldValue)
            obj.add(fieldName, Json.fromString(newValue)).asJson
          case None      => json
        }
    }

    case class ArrayOf(elementNav: JsonNav) extends JsonNav {
      def transformJson(json: Json, f: String => String): Json =
        json.asArray match {
          case Some(arr) =>
            val transformed = arr.map(elem => elementNav.transformJson(elem, f))
            Json.fromValues(transformed)
          case None      => json
        }
    }
  }

  // ============================================================================
  // Table Transformer
  // ============================================================================

  case class TableTransformer(columns: Seq[ColumnPlan]) {
    private val outputColumns: Set[String] = columns.map(_.columnName).toSet

    /** Transform a row using the column plans. Useful for testing. */
    def transform(row: Row): Row =
      columns.map { plan =>
        val result = plan match {
          case _: ColumnPlan.Passthrough => row.getOrElse(plan.columnName, "")
          case _: ColumnPlan.SetNull     => null
          case fixed: ColumnPlan.Fixed   => if (fixed.value == null) null else fixed.value.toString
          case t: ColumnPlan.Transform   => t.nav.wrap(t.f)(row.getOrElse(plan.columnName, ""))
          case d: ColumnPlan.Dependent   => d.f(row)(row.getOrElse(plan.columnName, ""))
        }
        plan.columnName -> result
      }.toMap

    def validateCovers(expectedColumns: Set[String]): Either[Set[String], Unit] = {
      val missing = expectedColumns -- outputColumns
      if (missing.isEmpty) Right(()) else Left(missing)
    }

    /** Get column names handled by this transformer */
    def columnNames: Set[String] = outputColumns
  }

  // ============================================================================
  // Unbound Transformer - can be bound to a column name
  // ============================================================================

  sealed trait UnboundTransformer {
    def bindTo(columnName: String): ColumnPlan
  }
  object UnboundTransformer       {

    /** Passthrough - preserves original database type */
    case object Passthrough extends UnboundTransformer {
      def bindTo(columnName: String): ColumnPlan = ColumnPlan.Passthrough(columnName)
    }

    /** Null - always inserts null */
    case object SetNull extends UnboundTransformer {
      def bindTo(columnName: String): ColumnPlan = ColumnPlan.SetNull(columnName)
    }

    /** Fixed value - inserts a constant value of any type */
    case class Fixed(value: Any) extends UnboundTransformer {
      def bindTo(columnName: String): ColumnPlan = ColumnPlan.Fixed(columnName, value)
    }

    /** String transformation with optional JSON navigation */
    case class Transform(nav: JsonNav, f: String => String) extends UnboundTransformer {
      def bindTo(columnName: String): ColumnPlan = ColumnPlan.Transform(columnName, nav, f)
    }

    /** Dependent transformation (depends on other column values) */
    case class Dependent(f: Row => String => String) extends UnboundTransformer {
      def bindTo(columnName: String): ColumnPlan = ColumnPlan.Dependent(columnName, f)
    }
  }

  // ============================================================================
  // ColumnRef - for dependency tracking
  // ============================================================================

  /** A reference to another column's value - used for dependent transformers */
  case class ColumnRef(name: String) {

    /** Transform with access to this column's value */
    def map(f: String => String => String): UnboundTransformer =
      UnboundTransformer.Dependent(row => f(row(name)))

    /** Combine with another column reference */
    def and(other: ColumnRef): ColumnRefs2 = ColumnRefs2(name, other.name)
  }

  case class ColumnRefs2(name1: String, name2: String) {
    def map(f: (String, String) => String => String): UnboundTransformer =
      UnboundTransformer.Dependent(row => f(row(name1), row(name2)))

    def and(other: ColumnRef): ColumnRefs3 = ColumnRefs3(name1, name2, other.name)
  }

  case class ColumnRefs3(name1: String, name2: String, name3: String) {
    def map(f: (String, String, String) => String => String): UnboundTransformer =
      UnboundTransformer.Dependent(row => f(row(name1), row(name2), row(name3)))
  }

  // ============================================================================
  // DSL
  // ============================================================================

  object DSL {

    import UnboundTransformer._

    /** Passthrough - preserves original database type (not just string identity) */
    val passthrough: UnboundTransformer = Passthrough

    /** Set to null */
    val setNull: UnboundTransformer = SetNull

    /** Fixed value of any type */
    def fixed[A](value: A): UnboundTransformer = Fixed(value)

    /** Apply a String => String transformation */
    def using(f: String => String): UnboundTransformer = Transform(JsonNav.Direct, f)

    /** Transform a field within JSON arrays */
    def jsonArray(field: String)(f: String => String): UnboundTransformer =
      Transform(JsonNav.ArrayOf(JsonNav.Field(field)), f)

    /** Reference another column for dependent transformers */
    def col(name: String): ColumnRef = ColumnRef(name)

    /** Build a table transformer from column bindings */
    def table(bindings: (String, UnboundTransformer)*): TableTransformer =
      TableTransformer(bindings.map { case (name, t) => t.bindTo(name) })
  }
}
