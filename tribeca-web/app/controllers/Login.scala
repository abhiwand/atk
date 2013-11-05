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

  var response: (Int, String) = (0,"");

    var login = Action(parse.json) {
        request =>{
            val auth = new Authorize(request.body, Providers.GooglePlus)
            response = getResponse(auth)
        }
          response._1 match {
            case StatusCodes.NOT_YET_REGISTERED => Ok(StatusCodes.getJsonStatusCode(StatusCodes.NOT_YET_REGISTERED))
            case StatusCodes.LOGIN => Ok(StatusCodes.getJsonStatusCode(StatusCodes.LOGIN)).withNewSession.withSession(SessionValName -> response._2)
            case StatusCodes.REGISTRATION_APPROVAL_PENDING =>
              Ok(StatusCodes.getJsonStatusCode(StatusCodes.REGISTRATION_APPROVAL_PENDING)).withCookies(Cookie("approvalPending","true", Some(3600),"/", None, true, false ))
            case _ => BadRequest("Couldn't validate auth response data")
          }
    }

    def getResponse(auth: Authorize): (Int, String) = {
        if (auth.validateUserInfo() == null)
            return (0,null)//BadRequest("Couldn't validate auth response data")

        val result = Users.login(auth.userInfo.email, MySQLStatementGenerator)
        val sessionId = Sessions.createSession(result.uid)

        result.errorCode match {
            case StatusCodes.NOT_YET_REGISTERED => (StatusCodes.NOT_YET_REGISTERED,null)//BadRequest(StatusCodes.getJsonStatusCode(StatusCodes.NOT_YET_REGISTERED))
            case StatusCodes.REGISTRATION_APPROVAL_PENDING => (StatusCodes.REGISTRATION_APPROVAL_PENDING,null)// Ok(StatusCodes.getJsonStatusCode(StatusCodes.REGISTRATION_APPROVAL_PENDING)).withNewSession.withSession(Se
            case _ => (StatusCodes.LOGIN, sessionId) //Ok(StatusCodes.getJsonStatusCode(StatusCodes.LOGIN)).withNewSession.withSession(SessionValName -> sessionId)
        }
    }

}
