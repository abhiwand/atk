//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package controllers

import play.api.mvc._
import play.api.libs.json._
import services.authorize.{Providers, Authorize}
import models._
import models.database._
import controllers.Session._
import models.Registrations
import models.RegistrationFormMapping
import play.api.mvc.Cookie
import scala.Some
import models.database.UserRow
import play.api.Play
import play.api.Play.current

/**
 * Singleton object to handle register request and generate response accordingly.
 */
object Register extends Controller {

    abstract class RegisterActionResponse
    case class SuccessfullyRegisterResponse(sessionId: String) extends RegisterActionResponse
    case class FailToValidateResponse() extends RegisterActionResponse
    case class GeneralErrorResponse() extends RegisterActionResponse

    var json: JsValue = _
    var auth: Authorize = _
    var response: RegisterActionResponse = _
    /**
     * register user to the system.
     */
    var register = Action {
        request => {
            Registrations.RegistrationFormValidation.bindFromRequest()(request).fold(
                formWithErrors => {
                    Redirect("/").withCookies(Cookie("authenticationFailed", "true", Some(3600), "/", None, true, false))
                },
                registrationForm => {
                    //make sure the terms are set to on since we couldnt' validate with a boolean
                    if (registrationForm.terms.trim.toLowerCase == "i agree" && registrationForm.experience >= 1 && registrationForm.experience <= 4) {
                        json = Json.parse(registrationForm.authResult)
                        auth = new Authorize(json, Providers.GooglePlus)
                        response = getResponse(registrationForm, auth, Sessions, MySQLStatementGenerator)
                    }else{
                      Redirect("/").withCookies(Cookie("authenticationFailed", "true", Some(3600), "/", None, true, false))
                    }
                }
            )

            response match {
                case successfulResponse: SuccessfullyRegisterResponse => Redirect("/ipython").withNewSession.withSession(SessionValName -> successfulResponse.sessionId).withCookies(getRegisteredCookie)
                case failedResponse: FailToValidateResponse => Redirect("/").withCookies(Cookie("authenticationFailed", "true", Some(3600),
                    "/", None, true, false))
                case generalErrorResponse: GeneralErrorResponse => Redirect("/").withCookies(Cookie("approvalPending", "true", Some(3600),
                    "/", None, true, false)).withCookies(getRegisteredCookie)
            }
        }


    }

    /**
     * Get registration response.
     * @param Authorization info
     * @return RegisterActionResponse
     */
    def getResponse(registrationForm: RegistrationFormMapping, auth: Authorize, sessionGen: SessionGenerator, statementGenerator: StatementGenerator): RegisterActionResponse = {
        if (auth.validateToken() == None || auth.validateUserInfo() == None)
            return FailToValidateResponse()

        val u = UserRow(None, auth.userInfo.get.givenName, auth.userInfo.get.familyName, auth.userInfo.get.email, true,
          Some(Play.application.configuration.getString("ipython.url").get),
          Some(Play.application.configuration.getString("ipython.secret").get), None)
        val result = Users.register(u, registrationForm, statementGenerator, DBRegisterCommand)

        if (result.login == 1) {
            val sessionId = sessionGen.create(result.uid)
            if (sessionId == None)
                FailToValidateResponse()
            else
                SuccessfullyRegisterResponse(sessionId.get)
        }
        else
            GeneralErrorResponse()
    }

    /**
     * get cookie to indicate that the user has registered.
     * @return cookie
     */
  def getRegisteredCookie:Cookie = {
    Cookie("registered", "true", Some(3600),"/", None, true, false)
  }

}