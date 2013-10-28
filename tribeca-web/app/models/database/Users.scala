package models.database

import play.api.db.slick.Config.driver.simple._

case class User(uid: Option[Long], givenName:String, familyName:String, email:String, phone:String,
                company:String, companyEmail:String, registered:Boolean, clusterId:Option[String])

class Users extends Table[User]("user_info") {
  def uid = column[Long]("uid", O.PrimaryKey, O.AutoInc)
  def givenName = column[String]("given_name")
  def familyName = column[String]("family_name")
  def email = column[String]("email", O.NotNull)
  def phone = column[String]("phone")
  def company = column[String]("organization_name")
  def companyEmail = column[String]("organization_email")
  //def picture = column[String]("picture")
  def registered = column[Boolean]("registered")
  def clusterId = column[String]("cluster_id", O.Nullable)
  def * = uid.? ~ givenName ~ familyName ~ email ~ phone ~ company ~ companyEmail ~ registered ~ clusterId.? <> (User, User.unapply _)
}
