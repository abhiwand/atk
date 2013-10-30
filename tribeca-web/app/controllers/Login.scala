package controllers

import play.api.mvc._
import services.authorize.{Providers, Authorize}
import models.database.{MySQLStatementGenerator, User}
import models.{StatusCodes, Sessions, Users}
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
        if (!(auth.valdiateTokenResponseData() && auth.validateToken() != null))
            return BadRequest("Couldn't validate auth response data")

        val result = Users.login(auth.userInfo.email, MySQLStatementGenerator)
        val sessionId = Sessions.createSession(result.uid)

        result.errorCode match {
            case StatusCodes.NOT_YET_REGISTERED => BadRequest(StatusCodes.getJsonStatusCode(StatusCodes.NOT_YET_REGISTERED))
            case StatusCodes.REGISTRATION_APPROVAL_PENDING => Ok(StatusCodes.getJsonStatusCode(StatusCodes.REGISTRATION_APPROVAL_PENDING)).withNewSession.withSession(SessionValName -> sessionId)
            case _ => Ok(StatusCodes.getJsonStatusCode(StatusCodes.LOGIN)).withNewSession.withSession(SessionValName -> sessionId)
        }
    }

}
