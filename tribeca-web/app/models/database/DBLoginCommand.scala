package models.database

import play.api.db.slick._
import java.sql.{ResultSet, Types}
import play.api.Play.current


object DBLoginCommand extends LoginCommand {

    def execute(email: String, statementGenerator: StatementGenerator): LoginOutput = DB.withSession {
        implicit session: scala.slick.session.Session =>

            val callString = "{call sp_login(?, ?, ?, ?)}";
            val cStmt = statementGenerator.getCallStatement(session, callString)
            cStmt.setString("email", email)
            cStmt.registerOutParameter("loginSuccessful", Types.BIGINT)
            cStmt.registerOutParameter("errorCode", Types.BIGINT)
            cStmt.registerOutParameter("errorMessage", Types.VARCHAR)
            val hadResults = cStmt.execute()

            val loginSuccessful = cStmt.getInt("loginSuccessful")
            val errorCode = cStmt.getInt("errorCode")
            val errorMessage = cStmt.getString("errorMessage")

            var uid = 0
            if (hadResults) {
                uid = getUidFromResultSet(cStmt.getResultSet())
            }

            val output = new LoginOutput(errorCode, errorMessage, loginSuccessful, uid)
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
