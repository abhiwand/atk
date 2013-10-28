package models

import play.api.Play.current
import anorm._
import play.api.db.DB

object test {
  def test(){ DB.withConnection { implicit  c =>
      val result = SQL("CALL `TRIBECA_WEB`.`sp_login`").execute()
      result
    }
  }

}
