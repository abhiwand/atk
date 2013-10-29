package controllers

import play.api.mvc._
import services.authorize.{Providers, Authorize}
import models.database.User
import models.{ErrorCodes, Users}
import play.api.libs.json.Json


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
        val u = User(None, userInfo.givenName, userInfo.familyName, userInfo.email, "Phone", "company", "companyemail", true, None)
        val result = Users.login(u)

        result.errorCode match {

            case ErrorCodes.NOT_YET_REGISTERED => Ok(Json.toJson("not yet register"))
            case ErrorCodes.REGISTRATION_APPROVAL_PENDING => Ok(Json.toJson("The user has registered and is in the waiting for approval."))
            case _ => Ok(Json.toJson("logged in."))
        }
    }

}
