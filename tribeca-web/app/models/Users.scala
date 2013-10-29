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

            val output = new RegistrationOutput(errorCode, errorMessage, loginAfterRegister, 0)

            if(hadResults) {
                val rs = cStmt.getResultSet();
                if(rs.next()) output.uid = rs.getInt("uid")
            }

            return output
    }
}
