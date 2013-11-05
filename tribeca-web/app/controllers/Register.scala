package controllers

import play.api.mvc._
import play.api.libs.json._
import services.authorize.{Providers, Authorize}
import play.api.data._
import play.api.data.Forms._
import models._
import models.database.{MySQLStatementGenerator, User}
import controllers.Session._
//import r.writeable

object Register extends Controller {

  var simpleResult: SimpleResult = Ok;
  var json: JsValue = _;
  var auth: Authorize = _;
  var response: (Int, String) = (0,"");
    var register = Action {
      request => {
          Registrations.RegistrationFormValidation.bindFromRequest()(request).fold(
            formWithErrors => {
              BadRequest("couldn't validate form vals")
            },
            registrationForm =>{
              //make sure the terms are set to on since we couldnt' validate with a boolean
              if(registrationForm.terms == "on" && registrationForm.experience >= 1 && registrationForm.experience <= 4){
                json = Json.parse(registrationForm.authResult)
                auth = new Authorize(json, Providers.GooglePlus)
                response = getResponse(json, registrationForm, auth)
              }
            }
          )
      }
      response._1 match{
        case  StatusCodes.ALREADY_REGISTER => Redirect("/ipython").withNewSession.withSession(SessionValName -> response._2)
        case  StatusCodes.LOGIN => Redirect("/ipython").withNewSession.withSession(SessionValName -> response._2)

        case  StatusCodes.REGISTRATION_APPROVAL_PENDING => Redirect("/").withCookies(Cookie("approvalPending","true", Some(3600),
          "/", None, true, false ))

        case _ => BadRequest("")
      }
    }

    def getResponse(req: JsValue, registrationForm: RegistrationFormMapping, auth: Authorize): (Int,String) = {
        if (auth.validateUserInfo() == null) return (0,null)

        val userInfo = auth.userInfo
        if(Users.exists(userInfo.email) && Whitelists.exists(userInfo.email)){
          val u = Users.readByEmail(userInfo.email)
          val sessionId = Sessions.create(u.uid.get)
          ( StatusCodes.ALREADY_REGISTER, sessionId)
        } else if(Users.exists(userInfo.email) && !Whitelists.exists(userInfo.email)){
          ( StatusCodes.REGISTRATION_APPROVAL_PENDING, "")
        } else{
          val u = User(None, userInfo.givenName, userInfo.familyName, userInfo.email, true, Some(""), None)
          val uid = Users.create(u)
          val registrationStatus = Registrations.createRegistration(database.Registration(uid,registrationForm.name,
            registrationForm.organization_name, registrationForm.organization_phone, registrationForm.organization_email,
            registrationForm.experience, registrationForm.role, registrationForm.whyParticipate, registrationForm.whatTools))
          if(Whitelists.exists(userInfo.email)){
            val sessionId = Sessions.create(uid)
            (StatusCodes.LOGIN, sessionId)
          } else{
            (StatusCodes.REGISTRATION_APPROVAL_PENDING,"")
          }
        }

        //TODO update procedure to take new registration fields
        //val result = Users.register(u, MySQLStatementGenerator)
      /*StatusCodes.REGISTRATION_APPROVAL_PENDING match {
          case StatusCodes.ALREADY_REGISTER => return (StatusCodes.ALREADY_REGISTER,sessionId)
          case StatusCodes.REGISTRATION_APPROVAL_PENDING => return (StatusCodes.REGISTRATION_APPROVAL_PENDING,sessionId)
          case _ => return (StatusCodes.LOGIN,sessionId)
        }*/
    }
}