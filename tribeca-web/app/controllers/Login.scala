package controllers

import play.api.mvc._
import services.authorize.{Providers, Authorize}
import models.database.{MySQLStatementGenerator, User}
import models.{Sessions, ErrorCodes, Users}
import play.api.libs.json.Json
import controllers.Session._
import models.database.User
import play.api.mvc.SimpleResult


object Login extends Controller {

    var login = Action(parse.json) {
        request =>
            val auth = new Authorize(request.body, Providers.GooglePlus)
            getResponse(auth)
    }


    def getResponse(auth: Authorize): SimpleResult = {

        if (!auth.isAuthResponseDataValid())
            return BadRequest("Couldn't validate auth response data")

        val userInfo = auth.getUserInfo()
        val u = User(None, userInfo.givenName, userInfo.familyName, userInfo.email, "Phone", "company", "companyemail", true, None, None)
        val result = Users.login(u, MySQLStatementGenerator)
        val sessionId = Sessions.createSession(result.uid)

        result.errorCode match {

            case ErrorCodes.NOT_YET_REGISTERED => Ok(Json.toJson("not yet register")).withNewSession.withSession(SessionValName -> sessionId)
            case ErrorCodes.REGISTRATION_APPROVAL_PENDING => Ok(Json.toJson("The user has registered and is in the waiting for approval.")).withNewSession.withSession(SessionValName -> sessionId)
            case _ => Ok(Json.toJson("logged in.")).withNewSession.withSession(SessionValName -> sessionId)
        }
    }

}
