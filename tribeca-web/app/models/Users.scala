package models

import models.database._
import play.api.Play.current
import play.api.db.slick.Config.driver.simple._
import play.api.db.slick.DB
import scala.slick.session.Session

import slick.driver.ExtendedProfile
import play.api.db.slick.Profile
import play.api.db.slick.DB

import java.sql.{Types, CallableStatement}

object Users {
    var table = new database.Users

    def register(user: User): RegistrationOutput = DB.withSession {
        implicit session: scala.slick.session.Session =>


            val cStmt: CallableStatement = session.conn.prepareCall("{call sp_register(?, ?, ?, ?, ?, ?, ?, ?, ?)}")
            cStmt.setString("given_name", user.givenName)
            cStmt.setString("family_name", user.familyName)
            cStmt.setString("email", user.email)
            cStmt.setString("phone", user.phone)
            cStmt.setString("organization_name", user.organization_name)
            cStmt.setString("organization_email", user.organization_email)
            cStmt.registerOutParameter("loginAfterRegister", Types.BIGINT)
            cStmt.registerOutParameter("errorCode", Types.BIGINT)
            cStmt.registerOutParameter("errorMessage", Types.VARCHAR)
            val hadResults = cStmt.execute()

            val loginAfterRegister = cStmt.getInt("loginAfterRegister")
            val errorCode = cStmt.getInt("errorCode")
            val errorMessage = cStmt.getString("errorMessage")


            var uid = 0
            if(hadResults) {
                val rs = cStmt.getResultSet();
                if(rs.next()) uid = rs.getInt("uid")
            }

            val output = new RegistrationOutput(errorCode, errorMessage, loginAfterRegister, uid)
            return output
    }

    def login(user: User) : LoginOutput   = DB.withSession {
        implicit session: scala.slick.session.Session =>


            val cStmt: CallableStatement = session.conn.prepareCall("{call sp_login(?, ?, ?, ?)}")
            cStmt.setString("email", user.email)
            cStmt.registerOutParameter("loginSuccessful", Types.BIGINT)
            cStmt.registerOutParameter("errorCode", Types.BIGINT)
            cStmt.registerOutParameter("errorMessage", Types.VARCHAR)
            val hadResults = cStmt.execute()

            val loginSuccessful = cStmt.getInt("loginSuccessful")
            val errorCode = cStmt.getInt("errorCode")
            val errorMessage = cStmt.getString("errorMessage")

            var uid = 0
            if(hadResults) {
                val rs = cStmt.getResultSet();
                if(rs.next()) uid = rs.getInt("uid")
            }

            val output = new LoginOutput(errorCode, errorMessage, loginSuccessful, uid)
            return output
    }

  def getUser(uid: Long): Query[Users, User] = DB.withSession{implicit session: scala.slick.session.Session =>
    return for { u <- table if u.uid === uid} yield u
  }
  //crud
  def readUser(uid: Long): database.User = DB.withSession{implicit session: scala.slick.session.Session =>
    val users = getUser(uid).list
    if(users.length > 0){
      return users.last
    }else{
      return null
    }
  }


  def insert(user: User): Long = DB.withSession{ implicit session: scala.slick.session.Session =>

    var uid = findByEmail(user.email)
    if(uid == 0){
      uid = table.insert(user)
    }
    return uid
  }

  val emailExist =for{
    email <- Parameters[String]
    u <- table if u.email === email
  }yield u

  def findByEmail(email: String): Long = DB.withSession {  implicit session: scala.slick.session.Session =>

    val test2 = emailExist(email).list
    if(test2.length > 0){
      return test2.last.uid.get
    }else{
      return 0
    }
  }
}
