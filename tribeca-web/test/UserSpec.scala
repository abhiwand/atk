import java.sql.{ResultSet, CallableStatement}
import models.database.{User, StatementGenerator}
import models.{RegistrationFormMapping, Users, StatusCodes}
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._
import org.specs2.mock._
import play.api.test._
import play.api.test.Helpers._
import scala.slick.session.Session


/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 10/29/13
 * Time: 4:07 PM
 * To change this template use File | Settings | File Templates.
 */
@RunWith(classOf[JUnitRunner])
class UserSpec extends Specification with Mockito {

    "User service should" should {

        val registrationForm = mock[RegistrationFormMapping]
        registrationForm.name returns "test name"
        registrationForm.organization_name returns "test org"
        registrationForm.organization_phone returns "123456789"
        registrationForm.organization_email returns "test@intel.com"
        registrationForm.experience returns 1
        registrationForm.role returns "Software Engineer"
        registrationForm.whyParticipate returns "explore big data"
        registrationForm.whatTools returns "test tool"

        "new register" in {

            running(FakeApplication()) {

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

                val u = User(None, "first name", "last name", "abcd@intel.com", true, None, None)
                val result = Users.register(u, registrationForm, dummyStatementGenerator)
                (result.uid == 100 && result.errorCode == 0 && result.errorMessage == "" && result.login == 0) must beEqualTo(true)
            }


            "already register" in {

                running(FakeApplication()) {

                    val dummyStatementGenerator = new StatementGenerator {
                        def getCallStatement(session: Session, callString: String): CallableStatement = {
                            val statement: CallableStatement = mock[CallableStatement]
                            statement.getInt("loginAfterRegister") returns 0
                            statement.getInt("errorCode") returns StatusCodes.ALREADY_REGISTER

                            val rs = mock[ResultSet]
                            rs.next() returns true
                            rs.getInt("uid") returns 100
                            statement.execute() returns true
                            statement.getResultSet() returns rs
                            return statement
                        }
                    }

                    val u = User(None, "first name", "last name", "abcd@intel.com", true, None, None)
                    val result = Users.register(u, registrationForm, dummyStatementGenerator)
                    (result.uid == 100 && result.errorCode == StatusCodes.ALREADY_REGISTER && result.login == 0) must beEqualTo(true)
                }

            }

            "log in success" in {

                running(FakeApplication()) {

                    val dummyStatementGenerator = new StatementGenerator {
                        def getCallStatement(session: Session, callString: String): CallableStatement = {
                            val statement: CallableStatement = mock[CallableStatement]
                            statement.getInt("loginSuccessful") returns 1
                            statement.getInt("errorCode") returns 0

                            val rs = mock[ResultSet]
                            rs.next() returns true
                            rs.getInt("uid") returns 100
                            statement.execute() returns true
                            statement.getResultSet() returns rs
                            return statement
                        }
                    }

                    val u = User(None, "first name", "last name", "abcd@intel.com", true, None, None)
                    val result = Users.login(u.email, dummyStatementGenerator)
                    (result.uid == 100 && result.errorCode == 0 && result.success == 1) must beEqualTo(true)
                }

            }

            "log in failed" in {

                running(FakeApplication()) {

                    val dummyStatementGenerator = new StatementGenerator {
                        def getCallStatement(session: Session, callString: String): CallableStatement = {
                            val statement: CallableStatement = mock[CallableStatement]
                            statement.getInt("loginSuccessful") returns 0
                            statement.getInt("errorCode") returns StatusCodes.NOT_YET_REGISTERED

                            val rs = mock[ResultSet]
                            rs.next() returns false
                            rs.getInt("uid") returns 100
                            statement.execute() returns true
                            statement.getResultSet() returns rs
                            return statement
                        }
                    }

                    val u = User(None, "first name", "last name", "abcd@intel.com", true, None, None)
                    val result = Users.login(u.email, dummyStatementGenerator)
                    (result.uid == 0 && result.errorCode == StatusCodes.NOT_YET_REGISTERED && result.success == 0) must beEqualTo(true)
                }

            }
        }
    }
}
