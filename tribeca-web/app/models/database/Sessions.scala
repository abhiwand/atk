package models.database

import play.api.db.slick.Config.driver.simple._

case class Session(Id:String, uid:Long, data:String)

class Sessions extends Table[Session]("Sessions"){
  def Id = column[String]("Id", O.PrimaryKey)
  def uid = column[Long]("uid", O.NotNull)
  def data = column[String]("data")
  def * = Id ~ uid ~ data <> (Session, Session.unapply _)
}
