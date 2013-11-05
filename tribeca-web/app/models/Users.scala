package models

import models.database._
import play.api.Play.current
import play.api.db.slick.Config.driver.simple._
import play.api.db.slick.DB
import scala.slick.session.Session

import slick.driver.ExtendedProfile
import play.api.db.slick.Profile
import play.api.db.slick.DB

import java.sql.{ResultSet, Types, CallableStatement}

object Users {

    def register(user: User, statementGenerator : StatementGenerator): RegistrationOutput = DB.withSession {
        implicit session: scala.slick.session.Session =>

            var callString =  "{call sp_register(?, ?, ?, ?, ?, ?, ?, ?, ?)}";
            var cStmt = statementGenerator.getCallStatement(session, callString)
            cStmt.setString("given_name", user.givenName)
            cStmt.setString("family_name", user.familyName)
            cStmt.setString("email", user.email)
            cStmt.setString("phone", "phone")
            cStmt.setString("organization_name", "org name")
            cStmt.setString("organization_email", "org email")
            cStmt.registerOutParameter("loginAfterRegister", Types.BIGINT)
            cStmt.registerOutParameter("errorCode", Types.BIGINT)
            cStmt.registerOutParameter("errorMessage", Types.VARCHAR)
            val hadResults = cStmt.execute()

            val loginAfterRegister = cStmt.getInt("loginAfterRegister")
            val errorCode = cStmt.getInt("errorCode")
            val errorMessage = cStmt.getString("errorMessage")

            var uid = 0
            if(hadResults) {
                uid = getUidFromResultSet(cStmt.getResultSet())
            }

            val output = new RegistrationOutput(errorCode, errorMessage, loginAfterRegister, uid)
            return output
    }

    private def getUidFromResultSet(rs: ResultSet): Int = {
        var uid = 0
        if (rs.next()) uid = rs.getInt("uid")
        return uid
    }

    def login(email:String, statementGenerator : StatementGenerator) : LoginOutput   = DB.withSession {
        implicit session: scala.slick.session.Session =>

            var callString =  "{call sp_login(?, ?, ?, ?)}";
            var cStmt = statementGenerator.getCallStatement(session, callString)
            cStmt.setString("email", email)
            cStmt.registerOutParameter("loginSuccessful", Types.BIGINT)
            cStmt.registerOutParameter("errorCode", Types.BIGINT)
            cStmt.registerOutParameter("errorMessage", Types.VARCHAR)
            val hadResults = cStmt.execute()

            val loginSuccessful = cStmt.getInt("loginSuccessful")
            val errorCode = cStmt.getInt("errorCode")
            val errorMessage = cStmt.getString("errorMessage")

            var uid = 0
            if(hadResults) {
                uid = getUidFromResultSet(cStmt.getResultSet())
            }

            val output = new LoginOutput(errorCode, errorMessage, loginSuccessful, uid)
            return output
    }

  def getByUid(uid: Long): Query[(database.Users.type , database.WhiteLists.type),(User,WhiteList)] = DB.withSession{implicit session: scala.slick.session.Session =>
    return for { (u,w) <- database.Users leftJoin database.WhiteLists on (_.uid === _.uid) if u.uid === uid} yield (u,w)
  }

  def getByEmail(email:String): Query[database.Users.type, database.User]  = DB.withSession{implicit session: scala.slick.session.Session =>
    for{ u <- database.Users if u.email === email }yield u
  }

  def anonymousUser(): User = {
    User(Some(0),"","","",false,None,None)
  }

  def exists(email:String): Boolean = DB.withSession{implicit session: scala.slick.session.Session =>
    val usersResult = readByEmail(email)
    if(usersResult != null && usersResult.uid.get > 0){
      true
    }else{
      false
    }
  }
  //crud
  def create(user:database.User): Long = DB.withSession{implicit session: scala.slick.session.Session =>
    database.Users.insert(user)
  }

  def readByUid(uid: Long): (database.User, database.WhiteList) = DB.withSession{implicit session: scala.slick.session.Session =>
    val users = getByUid(uid).list
    if(users.length > 0){
       users.last
    }else{
      null
    }
  }

  def readByEmail(email: String): database.User = DB.withSession{implicit session: scala.slick.session.Session =>
    val getResult = getByEmail(email).list
    if(getResult.length >0){
      getResult.last
    }else{
      null
    }
  }


}
