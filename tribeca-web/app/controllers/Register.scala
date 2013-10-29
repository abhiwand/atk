package controllers

import play.api.mvc._
import play.api.libs.json._
import services.authorize.{Providers, Authorize}

import models._
import models.database.User

object Register extends Controller {

    var register = Action(parse.json) {
        request =>
            val auth = new Authorize(request.body, Providers.GooglePlus)
            getResponse(auth)
    }

    def getResponse(auth: Authorize): SimpleResult = {

        if (!(auth.valdiateTokenResponseData() && auth.validateToken()))
            return BadRequest("Couldn't validate auth response data")

        val userInfo = auth.getUserInfo()
        if (userInfo == null)
            return BadRequest("Couldn't validate auth response data")

        val u = User(None, userInfo.givenName, userInfo.familyName, userInfo.email, "Phone", "company", "companyemail", true, None)
        var result = Users.register(u)

        result.errorCode match {

            case ErrorCodes.AlreadyRegister => Ok(Json.toJson("AlreadyRegistered"))
            case ErrorCodes.ApprovalPendingForRegistration => Ok(Json.toJson("The user has registered and is in the waiting for approval."))
            case _ => Ok(Json.toJson("Registered"))
        }
    }
}