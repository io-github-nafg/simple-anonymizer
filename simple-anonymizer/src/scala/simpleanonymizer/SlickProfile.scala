package simpleanonymizer

import com.github.tminglei.slickpg._
import slick.jdbc.meta.MQName

trait SlickProfile extends ExPostgresProfile with PgCirceJsonSupport {
  def pgjson = "jsonb"

  override val api: MyAPI.type = MyAPI

  object MyAPI extends ExtPostgresAPI with JsonImplicits

  def quoteQualified(schema: String, name: String): String =
    s"${quoteIdentifier(schema)}.${quoteIdentifier(name)}"

  def quoteQualified(mqName: MQName): String =
    mqName.schema match {
      case Some(s) => quoteQualified(s, mqName.name)
      case None    => quoteIdentifier(mqName.name)
    }
}

object SlickProfile extends SlickProfile
