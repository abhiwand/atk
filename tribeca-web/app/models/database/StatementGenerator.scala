package models.database

import java.sql.CallableStatement

/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 10/29/13
 * Time: 1:35 PM
 * To change this template use File | Settings | File Templates.
 */
trait StatementGenerator {
    def getCallStatement(session: scala.slick.session.Session, callString: String): CallableStatement
}
