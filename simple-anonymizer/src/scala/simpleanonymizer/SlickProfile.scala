package simpleanonymizer

import com.github.tminglei.slickpg._

trait SlickProfile extends ExPostgresProfile with PgCirceJsonSupport {
  def pgjson = "jsonb"

  override val api: MyAPI.type = MyAPI

  object MyAPI extends ExtPostgresAPI with JsonImplicits
}

object SlickProfile extends SlickProfile
