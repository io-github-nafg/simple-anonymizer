package simpleanonymizer

import org.slf4j.LoggerFactory

import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._

/** A lens for transforming string values, possibly within JSON structures.
  *
  * Lens describes how to apply a String => String transformation to a column value. Direct applies the function directly. JsonLens subtypes parse the column as
  * JSON and navigate to apply the function at the right location.
  *
  * Named "lens" because like optical lenses, these focus on a particular part of a structure and allow modification there.
  */
sealed trait Lens {

  /** Modify a column value by applying a string transformation at the focused location.
    *
    * @param f
    *   The transformation to apply to the focused string value(s)
    * @return
    *   A function that transforms the whole column value
    */
  def modify(f: String => String): String => String

  /** Like modify but works with Option[String] to handle null values. */
  def modifyOpt(f: Option[String] => Option[String]): Option[String] => Option[String]

  /** Lift a String => String transformation to work on JSON values. Used for composition. */
  protected def modifyJson(f: String => String): Json => Json
}

object Lens {
  private val logger = LoggerFactory.getLogger(getClass)

  /** Direct lens - applies the transformation directly to the string without JSON parsing.
    *
    * When used at the top level, this is the most efficient - no parsing or serialization. When nested inside ArrayElements, modifyJson extracts the string
    * from JSON and applies the transformation.
    */
  case object Direct extends Lens {
    override def modify(f: String => String): String => String = f

    override def modifyOpt(f: Option[String] => Option[String]): Option[String] => Option[String] = f

    /** Used when nested inside ArrayElements to transform JSON string values */
    protected def modifyJson(f: String => String): Json => Json = { json =>
      json.asString match {
        case None      =>
          logger.warn("Expected string but got {}", json.name)
          json
        case Some(str) =>
          val transformed = f(str)
          Json.fromString(transformed)
      }
    }
  }

  /** JsonLens subtypes parse the column as JSON and navigate within the structure. */
  sealed trait JsonLens extends Lens {
    override def modify(f: String => String): String => String = { jsonStr =>
      parse(jsonStr) match {
        case Right(json) => modifyJson(f)(json).noSpaces
        case Left(err)   =>
          logger.warn("Failed to parse JSON: {}", err.message)
          jsonStr
      }
    }

    override def modifyOpt(f: Option[String] => Option[String]): Option[String] => Option[String] = {
      val stringF: String => String = s => f(Some(s)).getOrElse(s)
      opt => opt.map(modify(stringF))
    }
  }

  /** Navigate into an object field and apply transformation there.
    *
    * @param fieldName
    *   The name of the field to navigate into
    * @param inner
    *   Optional inner lens for further navigation (e.g., nested objects)
    */
  case class Field(fieldName: String, inner: Lens = Direct) extends JsonLens {
    protected def modifyJson(f: String => String): Json => Json = { json =>
      json.asObject match {
        case None      =>
          logger.warn("Expected object but got {}", json.name)
          json
        case Some(obj) =>
          obj(fieldName) match {
            case Some(fieldValue) =>
              val transformed = inner.modifyJson(f)(fieldValue)
              obj.add(fieldName, transformed).asJson
            case None             =>
              logger.warn("Field '{}' not found in JSON object", fieldName)
              json
          }
      }
    }
  }

  /** Navigate into array elements and apply the inner lens to each.
    *
    * @param elementLens
    *   The lens to apply to each array element
    */
  case class ArrayElements(elementLens: Lens) extends JsonLens {
    protected def modifyJson(f: String => String): Json => Json = { json =>
      json.asArray match {
        case None      =>
          logger.warn("Expected array but got {}", json.name)
          json
        case Some(arr) =>
          val transformed = arr.map(elementLens.modifyJson(f))
          Json.fromValues(transformed)
      }
    }
  }
}
