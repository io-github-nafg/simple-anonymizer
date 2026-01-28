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

  type Row = Map[String, String]

  // ============================================================================
  // Result Kind - describes what kind of value a transformer produces
  // ============================================================================

  /** Describes what kind of result a column transformation produces.
    *
    * This enables DbSnapshot to handle different transformation types appropriately:
    *   - UseOriginal: preserve the original database value and type
    *   - SetNull: insert null
    *   - UseFixed: insert a fixed value
    *   - TransformString: apply a String => String function
    */
  sealed trait ResultKind
  object ResultKind {
    case object UseOriginal                                     extends ResultKind
    case object SetNull                                         extends ResultKind
    case class UseFixed(value: Any)                             extends ResultKind
    case class TransformString(f: String => String)             extends ResultKind
    case class TransformJson(nav: JsonNav, f: String => String) extends ResultKind
  }

  // ============================================================================
  // Value Transformers
  // ============================================================================

  sealed trait ValueTransformer {
    def apply(input: String): String
    def resultKind: ResultKind
  }
  object ValueTransformer       {
    case object PassThrough extends ValueTransformer {
      def apply(input: String): String = input
      def resultKind: ResultKind       = ResultKind.UseOriginal
    }

    case object SetNull extends ValueTransformer {
      def apply(input: String): String = null
      def resultKind: ResultKind       = ResultKind.SetNull
    }

    case class Fixed(value: Any) extends ValueTransformer {
      def apply(input: String): String = if (value == null) null else value.toString
      def resultKind: ResultKind       = ResultKind.UseFixed(value)
    }

    case class Simple(f: String => String) extends ValueTransformer {
      def apply(input: String): String = f(input)
      def resultKind: ResultKind       = ResultKind.TransformString(f)
    }
  }

  // ============================================================================
  // JSON Navigation
  // ============================================================================

  /** JsonNav works on JSON values internally to avoid repeated parse/encode cycles. Only the outermost layer converts to/from String.
    */
  sealed trait JsonNav {

    /** Transform a Json value, applying inner transformer to leaf string values */
    def transformJson(json: Json, inner: ValueTransformer): Json

    /** Wrap a ValueTransformer to work on JSON strings */
    def wrap(inner: ValueTransformer): ValueTransformer =
      ValueTransformer.Simple { jsonStr =>
        val json        = parse(jsonStr).getOrElse(Json.Null)
        val transformed = transformJson(json, inner)
        transformed.noSpaces
      }
  }
  object JsonNav {

    case object Direct extends JsonNav {
      def transformJson(json: Json, inner: ValueTransformer): Json = {
        val str         = json.asString.getOrElse("")
        val transformed = inner(str)
        Json.fromString(transformed)
      }

      // Override wrap for Direct to avoid an unnecessary JSON round-trip
      override def wrap(inner: ValueTransformer): ValueTransformer = inner
    }

    case class Field(fieldName: String) extends JsonNav {
      def transformJson(json: Json, inner: ValueTransformer): Json =
        json.asObject match {
          case Some(obj) =>
            val oldValue = obj(fieldName).flatMap(_.asString).getOrElse("")
            val newValue = inner(oldValue)
            obj.add(fieldName, Json.fromString(newValue)).asJson
          case None      => json
        }
    }

    case class ArrayOf(elementNav: JsonNav) extends JsonNav {
      def transformJson(json: Json, inner: ValueTransformer): Json =
        json.asArray match {
          case Some(arr) =>
            val transformed = arr.map(elem => elementNav.transformJson(elem, inner))
            Json.fromValues(transformed)
          case None      => json
        }
    }
  }

  // ============================================================================
  // Column Specification
  // ============================================================================

  sealed trait ColumnSpec {
    def columnName: String
    def dependsOn: Set[String]
    def transform(row: Row): String
    def resultKind: ResultKind
  }
  object ColumnSpec       {
    case class Independent(columnName: String, nav: JsonNav, transformer: ValueTransformer) extends ColumnSpec {
      def dependsOn: Set[String]      = Set.empty
      def transform(row: Row): String = {
        val originalValue = row.getOrElse(columnName, "")
        nav.wrap(transformer)(originalValue)
      }
      def resultKind: ResultKind      = nav match {
        case JsonNav.Direct => transformer.resultKind
        case _              =>
          // For JSON navigation, we always transform strings
          ResultKind.TransformJson(nav, transformer.apply)
      }
    }

    case class Dependent(columnName: String, dependencies: Set[String], nav: JsonNav, transformerFactory: Row => ValueTransformer) extends ColumnSpec {
      def dependsOn: Set[String]      = dependencies
      def transform(row: Row): String = {
        val originalValue = row.getOrElse(columnName, "")
        val transformer   = transformerFactory(row)
        nav.wrap(transformer)(originalValue)
      }
      // Dependent columns always do string transformation since the transformer is computed at runtime
      def resultKind: ResultKind      = ResultKind.TransformString(identity)
    }

    /** Passthrough column - preserves original database type */
    case class Passthrough(columnName: String) extends ColumnSpec {
      def dependsOn: Set[String]      = Set.empty
      def transform(row: Row): String = row.getOrElse(columnName, "")
      def resultKind: ResultKind      = ResultKind.UseOriginal
    }

    /** Null column - always inserts null */
    case class Null(columnName: String) extends ColumnSpec {
      def dependsOn: Set[String]      = Set.empty
      def transform(row: Row): String = null
      def resultKind: ResultKind      = ResultKind.SetNull
    }

    /** Fixed value column */
    case class FixedValue(columnName: String, value: Any) extends ColumnSpec {
      def dependsOn: Set[String]      = Set.empty
      def transform(row: Row): String = if (value == null) null else value.toString
      def resultKind: ResultKind      = ResultKind.UseFixed(value)
    }
  }

  // ============================================================================
  // Table Transformer
  // ============================================================================

  case class TableTransformer(columns: Seq[ColumnSpec]) {
    private val outputColumns: Set[String] = columns.map(_.columnName).toSet

    def transform(row: Row): Row = columns.map(spec => spec.columnName -> spec.transform(row)).toMap

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
    def bindTo(columnName: String): ColumnSpec
  }
  object UnboundTransformer       {

    import JsonNav.Direct

    /** Passthrough - preserves original database type */
    case object Passthrough extends UnboundTransformer {
      def bindTo(columnName: String): ColumnSpec = ColumnSpec.Passthrough(columnName)
    }

    /** Null - always inserts null */
    case object SetNull extends UnboundTransformer {
      def bindTo(columnName: String): ColumnSpec = ColumnSpec.Null(columnName)
    }

    /** Fixed value - inserts a constant value of any type */
    case class Fixed(value: Any) extends UnboundTransformer {
      def bindTo(columnName: String): ColumnSpec = ColumnSpec.FixedValue(columnName, value)
    }

    /** Simple string transformation */
    case class Simple(f: String => String) extends UnboundTransformer {
      def bindTo(columnName: String): ColumnSpec = ColumnSpec.Independent(columnName, Direct, ValueTransformer.Simple(f))
    }

    /** JSON navigation with string transformation */
    case class WithJson(nav: JsonNav, f: String => String) extends UnboundTransformer {
      def bindTo(columnName: String): ColumnSpec = ColumnSpec.Independent(columnName, nav, ValueTransformer.Simple(f))
    }

    /** Dependent transformation (depends on other column values) */
    case class Dependent(deps: Set[String], f: Row => String => String) extends UnboundTransformer {
      def bindTo(columnName: String): ColumnSpec =
        ColumnSpec.Dependent(columnName, deps, Direct, row => ValueTransformer.Simple(f(row)))
    }
  }

  // ============================================================================
  // ColumnRef - for dependency tracking
  // ============================================================================

  /** A reference to another column's value - used for dependent transformers */
  case class ColumnRef(name: String) {

    /** Transform with access to this column's value */
    def map(f: String => String => String): UnboundTransformer =
      UnboundTransformer.Dependent(Set(name), row => f(row(name)))

    /** Combine with another column reference */
    def and(other: ColumnRef): ColumnRefs2 = ColumnRefs2(name, other.name)
  }

  case class ColumnRefs2(name1: String, name2: String) {
    def map(f: (String, String) => String => String): UnboundTransformer =
      UnboundTransformer.Dependent(Set(name1, name2), row => f(row(name1), row(name2)))

    def and(other: ColumnRef): ColumnRefs3 = ColumnRefs3(name1, name2, other.name)
  }

  case class ColumnRefs3(name1: String, name2: String, name3: String) {
    def map(f: (String, String, String) => String => String): UnboundTransformer =
      UnboundTransformer.Dependent(Set(name1, name2, name3), row => f(row(name1), row(name2), row(name3)))
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
    def using(f: String => String): UnboundTransformer = Simple(f)

    /** Transform a field within JSON arrays */
    def jsonArray(field: String)(f: String => String): UnboundTransformer =
      WithJson(JsonNav.ArrayOf(JsonNav.Field(field)), f)

    /** Reference another column for dependent transformers */
    def col(name: String): ColumnRef = ColumnRef(name)

    /** Build a table transformer from column bindings */
    def table(bindings: (String, UnboundTransformer)*): TableTransformer =
      TableTransformer(bindings.map { case (name, t) => t.bindTo(name) })
  }
}
