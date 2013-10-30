package models.database

import play.api.db.slick.Config.driver.simple._

case class Session(Id:String, uid:Long, data:String, var timestamp:Long)

object Sessions extends Table[Session]("Sessions"){
  def Id = column[String]("Id", O.PrimaryKey)
  def uid = column[Long]("uid", O.NotNull)
  def data = column[String]("data")
  def timeStamp = column[Long]("timeStamp")
  def * = Id ~ uid ~ data ~ timeStamp <> (Session, Session.unapply _)
}
