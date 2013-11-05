package models

import play.api.Play.current
import play.api.db.slick.Config.driver.simple._
import play.api.db.slick.DB
import models.database.Session
import scala.Some

object Whitelists {
  def anonymousWhitelist(): database.WhiteList = {
    database.WhiteList(Some(0),Some(""))
  }

  def exists(email:String ): Boolean = {
    val whiteListResult = read(email)
    if(whiteListResult != null && whiteListResult.uid.get > 0 ){
      true
    } else{
      false
    }
  }
  def getByEmail(email:String): Query[database.WhiteLists.type , database.WhiteList] = DB.withSession{implicit session: scala.slick.session.Session =>
   for { w <- database.WhiteLists if w.email === email }yield w
  }

  //crud
  def read(email:String): database.WhiteList = DB.withSession{implicit session: scala.slick.session.Session =>
    val result = getByEmail(email).list
    if( result != null && result.length > 0){
      result.last
    } else{
      null
    }
  }
}
