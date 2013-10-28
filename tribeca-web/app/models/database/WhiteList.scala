package models.database

import play.api.db.slick.Config.driver.simple._

case class WhiteList(id:Option[Long], email:String)

class WhiteLists extends Table[WhiteList]("white_list"){
  def uid = column[(Long)]("uid", O.PrimaryKey)
  def email = column[(String)]("email", O.NotNull)
  def * = uid.? ~ email <> (WhiteList , WhiteList.unapply _)
  //def autoInc = * returning id
}
