package controllers

import play.api.mvc._
import play.api.libs.json._
import services.authorize.{Providers, Authorize}

import models._
import models.database.{MySQLStatementGenerator, User}
import controllers.Session._

object Register extends Controller {

    var register = Action(parse.json) {
        request =>

            val auth = new Authorize(request.body \ "auth", Providers.GooglePlus)
            getResponse(request, auth)
    }

    def getResponse(req: Request[JsValue], auth: Authorize): SimpleResult = {

        if (!auth.isAuthResponseDataValid())
            return BadRequest("Couldn't validate auth response data")

        val userInfo = auth.getUserInfo()
        val u = User(None, userInfo.givenName, userInfo.familyName, userInfo.email, "Phone", "company", "companyemail", true, None)
        val result = Users.register(u, MySQLStatementGenerator)
        val sessionId = Sessions.createSession(result.uid)
        //Sessions.removeSession(sessionId)
        result.errorCode match {

            case ErrorCodes.ALREADY_REGISTER => Ok(Json.toJson("AlreadyRegistered")).withNewSession.withSession(SessionValName -> sessionId)
            case ErrorCodes.REGISTRATION_APPROVAL_PENDING => Ok(Json.toJson("The user has registered and is in the waiting for approval.")).withNewSession.withSession(SessionValName -> sessionId)
            case _ => Ok(Json.toJson("Registered")).withNewSession.withSession(SessionValName -> sessionId)
        }
    }
}