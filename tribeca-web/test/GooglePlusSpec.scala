import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import play.api.libs.json.{Json, JsValue}
import play.api.Play
import play.api.test.FakeApplication
import play.api.test.Helpers._
import scala.None
import services.authorize.providers.google.GooglePlus


/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 11/7/13
 * Time: 11:14 AM
 * To change this template use File | Settings | File Templates.
 */
@RunWith(classOf[JUnitRunner])
class GooglePlusSpec extends Specification with Mockito {
    "Validate auth data" should {
        "validate empty user info" in {
            running(FakeApplication()) {
                val json: JsValue = Json.parse("{}")
                GooglePlus.validateUserInfo(json) should beEqualTo(None)
            }
        }

        "validate user info" in {
            running(FakeApplication()) {
                val json: JsValue = Json.parse("{\"id\":\"12345678\",\"email\":\"test@gmail.com\",\"verified_email\":true,\"name\":\"random user\",\"given_name\":\"random\",\"family_name\":\"User\",\"link\":\"https://plus.google.com/111233\",\"picture\":\"https://lh6.googleusercontent.com/-bdg2Eq5sa80/2222/1111/A9c7A4f2di8/photo.jpg\",\"locale\":\"en\"}")
                (GooglePlus.validateUserInfo(json) == None) should beFalse
            }
        }


        "validate user info - changed order in json nodes" in {
            running(FakeApplication()) {
                val json: JsValue = Json.parse("{\"id\":\"12345678\",\"verified_email\":true,\"email\":\"test@gmail.com\",\"name\":\"random user\",\"family_name\":\"User\",\"link\":\"https://plus.google.com/111233\",\"given_name\":\"random\",\"picture\":\"https://lh6.googleusercontent.com/-bdg2Eq5sa80/2222/1111/A9c7A4f2di8/photo.jpg\",\"locale\":\"en\"}")
                (GooglePlus.validateUserInfo(json) == None) should beFalse
            }
        }

        "check client id valid" in {
            running(FakeApplication()) {
                GooglePlus.validateClientId(GooglePlus.clientId) should beEqualTo(true)
            }
        }

        "check client id invalid" in {
            running(FakeApplication()) {
                GooglePlus.validateClientId(GooglePlus.clientId + "123") should beEqualTo(false)
            }
        }
    }

}
