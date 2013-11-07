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
import models.database.{StatementGenerator, MySQLStatementGenerator, UserRow}
import controllers.Session._

/**
 * Singleton object to handle register request and generate response accordingly.
 */
object Register extends Controller {

  var json: JsValue = _
  var auth: Authorize = _
  var response: (Int, Option[String]) = (0, None)

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
                response = getResponse(registrationForm, auth, Sessions, MySQLStatementGenerator)
              }
            }
          )
      }

      response._1 match{
        case  StatusCodes.LOGIN => Redirect("/ipython").withNewSession.withSession(SessionValName -> response._2.get)
        case  StatusCodes.REGISTRATION_APPROVAL_PENDING => Redirect("/").withCookies(Cookie("approvalPending","true", Some(3600),
            "/", None, true, false ))

        case _ => Redirect("/").withCookies(Cookie("authenticationFailed","true", Some(3600),
            "/", None, true, false ))
      }
    }

    /**
     *
     * @param Authorization info
     * @return tuple of (status code, session Id)
     */
    def getResponse(registrationForm: RegistrationFormMapping, auth: Authorize, sessionGen: SessionGenerator, statementGenerator: StatementGenerator): (Int, Option[String]) = {

        if (auth.validateUserInfo() == None)
            return (StatusCodes.FAIL_TO_VALIDATE_AUTH_DATA, None)

        val u = UserRow(None, auth.userInfo.get.givenName, auth.userInfo.get.familyName, auth.userInfo.get.email, true, Some(""), None, None)
        val result = Users.register(u, registrationForm, statementGenerator)

        if (result.login == 1)
            (StatusCodes.LOGIN, Some(sessionGen.create(result.uid)))
        else
            (result.errorCode, None)
    }
}