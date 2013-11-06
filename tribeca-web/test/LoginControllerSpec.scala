import controllers.Login
import java.sql.{ResultSet, CallableStatement}
import models.database.StatementGenerator
import models.{StatusCodes, SessionGenerator}
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import play.api.test.FakeApplication
import play.api.test.Helpers._
import scala.slick.session.Session
import scala.Some
import services.authorize.{UserInfo, Authorize}

/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 11/6/13
 * Time: 11:10 AM
 * To change this template use File | Settings | File Templates.
 */
@RunWith(classOf[JUnitRunner])
class LoginControllerSpec extends Specification with Mockito {
    "Login controller get response" should {
        "get response for invaid auth info" in {

            val auth = mock[Authorize]
            auth.validateUserInfo() returns None
            val sessionGen = mock[SessionGenerator]
            sessionGen.create(1) returns "1"
            val statementGenerator = mock[StatementGenerator]
            val response = Login.getResponse(auth, sessionGen, statementGenerator)
            response._1 must beEqualTo(StatusCodes.FAIL_TO_VALIDATE_AUTH_DATA)
        }

        "get response for valid log in" in {

            running(FakeApplication()) {
                val userInfo = mock[UserInfo]
                val auth = mock[Authorize]
                auth.validateUserInfo() returns Some(userInfo)
                auth.userInfo returns Some(userInfo)

                val sessionGen = mock[SessionGenerator]
                sessionGen.create(100) returns "100"

                val dummyStatementGenerator = new StatementGenerator {
                    def getCallStatement(session: Session, callString: String): CallableStatement = {
                        val statement: CallableStatement = mock[CallableStatement]
                        statement.getInt("loginSuccessful") returns 1
                        statement.getInt("errorCode") returns 0
                        statement.getString("errorMessage") returns ""

                        val rs = mock[ResultSet]
                        rs.next() returns true
                        rs.getInt("uid") returns 100
                        statement.execute() returns true
                        statement.getResultSet() returns rs
                        return statement
                    }
                }

                val response = Login.getResponse(auth, sessionGen, dummyStatementGenerator)
                (response._1 == StatusCodes.LOGIN && response._2 == Some("100")) must beEqualTo(true)
            }

        }


        "get response for approval pending" in {

            running(FakeApplication()) {
                val userInfo = mock[UserInfo]
                val auth = mock[Authorize]
                auth.validateUserInfo() returns Some(userInfo)
                auth.userInfo returns Some(userInfo)

                val sessionGen = mock[SessionGenerator]
                sessionGen.create(100) returns "100"

                val dummyStatementGenerator = new StatementGenerator {
                    def getCallStatement(session: Session, callString: String): CallableStatement = {
                        val statement: CallableStatement = mock[CallableStatement]
                        statement.getInt("loginSuccessful") returns 0
                        statement.getInt("errorCode") returns StatusCodes.REGISTRATION_APPROVAL_PENDING
                        statement.getString("errorMessage") returns ""

                        val rs = mock[ResultSet]
                        rs.next() returns true
                        rs.getInt("uid") returns 100
                        statement.execute() returns true
                        statement.getResultSet() returns rs
                        return statement
                    }
                }

                val response = Login.getResponse(auth, sessionGen, dummyStatementGenerator)
                (response._1 == StatusCodes.REGISTRATION_APPROVAL_PENDING && response._2 == None) must beEqualTo(true)
            }

        }
    }


}
