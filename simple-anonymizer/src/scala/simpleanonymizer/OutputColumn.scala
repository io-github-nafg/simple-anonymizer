package simpleanonymizer

import scala.language.dynamics

/** Base trait for anything that can be an output column */
trait OutputColumn {
  def name: String

  private[simpleanonymizer] def transform(wrapIfJson: AnyRef => AnyRef): RawRow => AnyRef
}

object OutputColumn {

  /** A JSON field with a transformation applied */
  class JsonFieldTransformed(val fieldName: String, val f: String => String)

  /** A reference to a field within a JSON element */
  class JsonFieldRef(val fieldName: String) {

    /** Apply a String => String transformation to this JSON field */
    def mapString(f: String => String): JsonFieldTransformed = new JsonFieldTransformed(fieldName, f)
  }

  /** Dynamic accessor for JSON element fields within mapJsonArray */
  class JsonObject extends Dynamic {
    def selectDynamic(fieldName: String): JsonFieldRef = new JsonFieldRef(fieldName)
  }

  /** A reference to a source column - can be used directly as output (passthrough) or transformed */
  case class SourceColumn(name: String) extends OutputColumn {

    /** Apply a String => String transformation. Null values are preserved (the function is not called for nulls). */
    def mapString(f: String => String): TransformedColumn =
      TransformedColumn(name, Lens.Direct, _ => opt => opt.map(f))

    /** Apply an Option[String] => Option[String] transformation, for when you need to handle null values explicitly. */
    def mapOptString(f: Option[String] => Option[String]): TransformedColumn =
      TransformedColumn(name, Lens.Direct, _ => f)

    /** Transform elements within a JSON array - the lambda receives a JsonObject for field access */
    def mapJsonArray(f: JsonObject => JsonFieldTransformed): TransformedColumn = {
      val element          = new JsonObject
      val fieldTransformed = f(element)
      val lens             = Lens.ArrayElements(Lens.Field(fieldTransformed.fieldName))
      TransformedColumn(name, lens, _ => opt => opt.map(fieldTransformed.f))
    }

    /** Set this column to null */
    def nulled = FixedColumn(name, null)

    /** Set this column to a fixed value */
    def :=[A](value: A): FixedColumn = FixedColumn(name, value)

    override private[simpleanonymizer] def transform(wrapIfJson: AnyRef => AnyRef): RawRow => AnyRef =
      rawRow => wrapIfJson(rawRow.objects.getOrElse(name, null))
  }

  /** A transformed column with a function applied */
  case class TransformedColumn(name: String, lens: Lens, f: RawRow => Option[String] => Option[String]) extends OutputColumn {
    override private[simpleanonymizer] def transform(wrapIfJson: AnyRef => AnyRef): RawRow => AnyRef = { rawRow =>
      val str         = rawRow.strings.get(name).filter(_ != null)
      val f0          = lens.modifyOpt(f(rawRow))
      val transformed = f0(str)
      transformed match {
        case None    => null
        case Some(s) => wrapIfJson(s)
      }
    }
  }

  /** A column set to a fixed value */
  case class FixedColumn(name: String, value: Any) extends OutputColumn {
    override private[simpleanonymizer] def transform(wrapIfJson: AnyRef => AnyRef): RawRow => AnyRef = {
      val wrappedValue = wrapIfJson(value.asInstanceOf[AnyRef])
      _ => wrappedValue
    }
  }
}
