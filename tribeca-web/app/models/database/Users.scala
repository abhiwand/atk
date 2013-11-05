package models.database

import play.api.db.slick.Config.driver.simple._

case class User(uid: Option[Long], givenName: String, familyName: String, email: String, registered: Boolean,
                ipythonUrl: Option[String], clusterId: Option[String])

object Users extends Table[User]("user_info") {

    def uid = column[Long]("uid", O.PrimaryKey, O.AutoInc)

    def givenName = column[String]("given_name")

    def familyName = column[String]("family_name")

    def email = column[String]("email", O.NotNull)

    def registered = column[Boolean]("registered")

    def ipythonUrl = column[String]("ipythonUrl")

    def clusterId = column[String]("cluster_id", O.Nullable)

    def * = uid.? ~ givenName ~ familyName ~ email ~ registered ~ ipythonUrl.? ~ clusterId.? <>(User, User.unapply _)
}



