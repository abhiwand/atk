import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import services.authorize.CookieGenerator

@RunWith(classOf[JUnitRunner])
class CookieSpec extends Specification with Mockito {
    "Cookie gen" should {
        "check generated value" in {

            val cookieGen = spy(new CookieGenerator)
            cookieGen.getEpochTime returns 1383251539
            cookieGen.create_signed_value("secret", "username-10.10.68.142-8888", "localUser") must beEqualTo("bG9jYWxVc2Vy|1383251539|4fa196a04302d35ac76d65bd1cf01d0c24ac44e1")
        }

        "Secret cannot be null" in {
            val cookieGen = new CookieGenerator
            var isSuccess: Boolean = true
            try {
                cookieGen.create_signed_value(null, "username-10.10.68.142-8888", "localUser")
            }
            catch {
                case ex: IllegalArgumentException => {
                    isSuccess = false
                }
            }

            isSuccess must beEqualTo(false)
        }

        "Secret cannot be empty" in {
            val cookieGen = new CookieGenerator
            var isSuccess: Boolean = true
            try {
                cookieGen.create_signed_value("", "username-10.10.68.142-8888", "localUser")
            }
            catch {
                case ex: IllegalArgumentException => {
                    isSuccess = false
                }
            }

            isSuccess must beEqualTo(false)
        }
    }
}

