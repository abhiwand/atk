package models.database

import play.api.db.slick._
import models.RegistrationFormMapping
import java.sql.{ResultSet, Types}
import play.api.Play.current

/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 11/8/13
 * Time: 10:40 AM
 * To change this template use File | Settings | File Templates.
 */
object DBRegisterCommand extends RegisterCommand {

    def execute(user: UserRow, registrationForm: RegistrationFormMapping, statementGenerator: StatementGenerator): RegistrationOutput = DB.withSession {
        implicit session: scala.slick.session.Session =>

            val callString = "{call sp_register(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}";
            val cStmt = statementGenerator.getCallStatement(session, callString)
            cStmt.setString("myName", registrationForm.name)
            cStmt.setString("given_name", user.givenName)
            cStmt.setString("family_name", user.familyName)
            cStmt.setString("email", user.email)
            cStmt.setString("organization_name", registrationForm.organization_name)
            cStmt.setString("organization_phone", registrationForm.organization_phone)
            cStmt.setString("organization_email", "")
            cStmt.setInt("experience", registrationForm.experience)
            cStmt.setString("role", registrationForm.role)
            cStmt.setString("why_participate", registrationForm.whyParticipate)
            cStmt.setString("what_tools", registrationForm.whatTools)
            cStmt.registerOutParameter("loginAfterRegister", Types.BIGINT)
            cStmt.registerOutParameter("errorCode", Types.BIGINT)
            cStmt.registerOutParameter("errorMessage", Types.VARCHAR)
            val hadResults = cStmt.execute()

            val loginAfterRegister = cStmt.getInt("loginAfterRegister")
            val errorCode = cStmt.getInt("errorCode")
            val errorMessage = cStmt.getString("errorMessage")

            var uid = 0
            if (hadResults) {
                uid = getUidFromResultSet(cStmt.getResultSet())
            }

            val output = new RegistrationOutput(errorCode, errorMessage, loginAfterRegister, uid)
            return output
    }

    /**
     *
     * @param rs
     * @return
     */
    private def getUidFromResultSet(rs: ResultSet): Int = {
        var uid = 0
        if (rs.next()) uid = rs.getInt("uid")
        return uid
    }
}
