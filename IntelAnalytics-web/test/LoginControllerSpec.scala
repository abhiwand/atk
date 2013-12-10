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
import play.mvc.Http.Context
import scala.slick.session.Session
import scala.Some
import services.authorize.{UserInfo, Authorize}

@RunWith(classOf[JUnitRunner])
class LoginControllerSpec extends Specification with Mockito {
    "Login controller get response" should {
        "get response for invaid auth info" in {

            val auth = mock[Authorize]
            auth.validateUserInfo() returns None
            val sessionGen = mock[SessionGenerator]
            sessionGen.create(1) returns Some("1")
            val statementGenerator = mock[StatementGenerator]
            val result = Login.getResult(auth, sessionGen, statementGenerator)
            (result.header.status == 200) must
              beTrue
        }

        "get response for valid log in" in {

            running(FakeApplication()) {
                val userInfo = mock[UserInfo]
                val auth = mock[Authorize]
                auth.validateUserInfo() returns Some(userInfo)
                auth.userInfo returns Some(userInfo)

                val sessionGen = mock[SessionGenerator]
                sessionGen.create(100) returns Some("100")

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

                val result = Login.getResult(auth, sessionGen, dummyStatementGenerator)
                (result.header.status == 200 && result.header.headers("Set-Cookie").contains("PLAY_SESSION")) must beEqualTo(true)
            }

        }


        "get response for approval pending" in {

            running(FakeApplication()) {
                val userInfo = mock[UserInfo]
                val auth = mock[Authorize]
                auth.validateUserInfo() returns Some(userInfo)
                auth.userInfo returns Some(userInfo)

                val sessionGen = mock[SessionGenerator]
                sessionGen.create(100) returns Some("100")

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

                val result = Login.getResult(auth, sessionGen, dummyStatementGenerator)
                (result.header.status == 200 && !result.header.headers.contains("Set-Cookie")) must beTrue
            }

        }
    }


}
