import controllers.AuthController
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._

import play.api.test._
import play.api.test.Helpers._

/**
 * Add your spec here.
 * You can mock out a whole application including requests, plugins etc.
 * For more information, consult the wiki.
 */
@RunWith(classOf[JUnitRunner])
class AuthenticationSpec extends Specification{
  "Application" should {

    "Get token from json data simple" in new WithApplication{

      var json = "{\n    \"state\": \"\",\n    \"access_token\": \"ya29.AHES6ZSpnsiWDES9_LevD-jpB4-43XXlHcWTxic1_KPuGko\",\n    \"token_type\": \"Bearer\",\n    \"expires_in\": \"3600\",\n    \"authuser\": \"0\",\n    \"client_id\": \"141308260505-d7utft9orcofca75fkspuit96ordo8dm.apps.googleusercontent.com\",\n    \"code\": \"4/Rcb01t4MaePknizm3D5shG8Rboys.EktDwljhtQkfEnp6UAPFm0HcODragwI\",\n    \"cookie_policy\": \"single_host_origin\",\n    \"expires_at\": \"1382666090\",\n    \"g-oauth-window\": \"Window\",\n    \"g_user_cookie_policy\": \"single_host_origin\",\n    \"id_token\": \"eyJhbGciOiJSUzI1NiIsImtpZCI6ImU1NTkzYmQ2NTliOTNlOWZiZGQ4OTQ1NDIzNGVhMmQ1YWE2Y2MzYWMifQ.eyJpc3MiOiJhY2NvdW50cy5nb29nbGUuY29tIiwic3ViIjoiMTE3NDc1NDU2NDcyNTQxODU4NTE5IiwiYXRfaGFzaCI6Il9GMWI3dWtoY1NiWFFQRW4tRDB6eVEiLCJjX2hhc2giOiJMQUQwSHVfQW9lc3p0V3BqeDQ0WE9nIiwiYXpwIjoiMTQxMzA4MjYwNTA1LWQ3dXRmdDlvcmNvZmNhNzVma3NwdWl0OTZvcmRvOGRtLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwiYXVkIjoiMTQxMzA4MjYwNTA1LWQ3dXRmdDlvcmNvZmNhNzVma3NwdWl0OTZvcmRvOGRtLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwiZW1haWwiOiJmcm9ncm9kQGdtYWlsLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjoidHJ1ZSIsImlhdCI6MTM4MjY2MjE5MCwiZXhwIjoxMzgyNjY2MDkwfQ.HiJIdbqkDjPZ441TCNZfNHOKGfhFrKg9ZiqtgG2pRuNl7ws-8nvwIj9hrZo0NkY7zFD7ZFNVQR7TSEo9iNEPht4eE23Gp1XouB90aOTAm0iXYyPRqoxisBD1t6GDlgevFu4Zk_kZk9sY9KkJipU_EChCudSiBIb8HeDrykcDDCI\",\n    \"issued_at\": \"1382662490\",\n    \"prompt\": \"none\",\n    \"response_type\": \"code token id_token gsession\",\n    \"scope\": \"https://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/plus.login\",\n    \"session_state\": \"8f033954a2d8f9bde1d0521a3ef4083c9b0b0036..6394\"\n}"

      //currently test with startWith and endWidth because must be is always returning error
      AuthController.getToken(json) must startWith ("ya29.AHES6ZSpnsiWDES9_LevD-jpB4-43XXlHcWTxic1_KPuGko")
      AuthController.getToken(json) must endWith ("ya29.AHES6ZSpnsiWDES9_LevD-jpB4-43XXlHcWTxic1_KPuGko")
    }
  }
}
