import controllers.Register
import controllers.Register.{FailToValidateResponse, GeneralErrorResponse, SuccessfullyRegisterResponse}
import java.sql.{ResultSet, CallableStatement}
import models.database.StatementGenerator
import models._
import models.RegistrationFormMapping
import models.StatusCodes
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import play.api.test.FakeApplication
import play.api.test.FakeApplication
import play.api.test.Helpers._
import scala.slick.session.Session
import scala.Some
import services.authorize.UserInfo
import services.authorize.{UserInfo, Authorize}

/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 11/6/13
 * Time: 2:24 PM
 * To change this template use File | Settings | File Templates.
 */
@RunWith(classOf[JUnitRunner])
class RegisterControllerSpec extends Specification with Mockito {
    "Register controller get response" should {

        val registrationData = RegistrationFormMapping("name", "Intel", "503-111-1111","email@something.com",
            2, "Software Engineer", "Explore big data solutions",
            "concept 1", "term content", "Auth result content")

        "Invalid auth" in {
            val auth = mock[Authorize]
            auth.validateUserInfo() returns None
            val sessionGen = mock[SessionGenerator]
            sessionGen.create(1) returns Some("1")
            val statementGenerator = mock[StatementGenerator]

            val result = Register.getResponse(registrationData, auth, sessionGen, statementGenerator)

            result.isInstanceOf[FailToValidateResponse] must beTrue
        }

        "User already in white list" in {
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
                        statement.getInt("loginAfterRegister") returns 1
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

                val result = Register.getResponse(registrationData, auth, sessionGen, dummyStatementGenerator)
                result.isInstanceOf[SuccessfullyRegisterResponse] must beTrue
                "100" must beEqualTo (result.asInstanceOf[SuccessfullyRegisterResponse].sessionId)
            }


        }

        "User needs to wait for approval" in {
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
                        statement.getInt("loginAfterRegister") returns 0
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

                val result = Register.getResponse(registrationData, auth, sessionGen, dummyStatementGenerator)
                result.isInstanceOf[GeneralErrorResponse] must beTrue
            }
        }


        "User needs to wait for approval / repeated register" in {
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
                        statement.getInt("loginAfterRegister") returns 0
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

                val result = Register.getResponse(registrationData, auth, sessionGen, dummyStatementGenerator)
                result.isInstanceOf[GeneralErrorResponse] must beTrue
            }
        }

        "User already in white list / repeated register" in {
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
                        statement.getInt("loginAfterRegister") returns 1
                        statement.getInt("errorCode") returns StatusCodes.ALREADY_REGISTER_AND_ALREADY_IN_WHITE_LIST
                        statement.getString("errorMessage") returns ""

                        val rs = mock[ResultSet]
                        rs.next() returns true
                        rs.getInt("uid") returns 100
                        statement.execute() returns true
                        statement.getResultSet() returns rs
                        return statement
                    }
                }

                val result = Register.getResponse(registrationData, auth, sessionGen, dummyStatementGenerator)
                result.isInstanceOf[SuccessfullyRegisterResponse] must beTrue
                "100" must beEqualTo (result.asInstanceOf[SuccessfullyRegisterResponse].sessionId)
            }
        }

    }
}
