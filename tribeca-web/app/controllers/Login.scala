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
import services.authorize.{Providers, Authorize}
import models.database.MySQLStatementGenerator
import models.{StatusCodes, Sessions, Users}
import controllers.Session._

/**
 * Singleton object to handle log in request and generate response accordingly.
 */
object Login extends Controller {

    var response: (Int, Option[String]) = (0, None);

    var login = Action(parse.json) {
        request => {
            val auth = new Authorize(request.body, Providers.GooglePlus)
            response = getResponse(auth)
        }
            response._1 match {
                case StatusCodes.NOT_YET_REGISTERED => Ok(StatusCodes.getJsonStatusCode(StatusCodes.NOT_YET_REGISTERED))
                case StatusCodes.LOGIN => Ok(StatusCodes.getJsonStatusCode(StatusCodes.LOGIN)).withNewSession.withSession(SessionValName -> response._2.toString)
                case StatusCodes.REGISTRATION_APPROVAL_PENDING =>
                    Ok(StatusCodes.getJsonStatusCode(StatusCodes.REGISTRATION_APPROVAL_PENDING)).withCookies(Cookie("approvalPending", "true", Some(3600), "/", None, true, false))
                case _ => BadRequest("Couldn't validate auth response data")
            }
    }

    /**
     *
     * @param authorization info
     * @return tuple of (status code, session Id)
     */
    def getResponse(auth: Authorize): (Int, Option[String]) = {
        if (Option(auth.validateUserInfo()) == None)
            return (0, None)

        val result = Users.login(auth.userInfo.email, MySQLStatementGenerator)
        val sessionId = Sessions.create(result.uid)

        result.errorCode match {
            case StatusCodes.NOT_YET_REGISTERED => (StatusCodes.NOT_YET_REGISTERED, None)
            case StatusCodes.REGISTRATION_APPROVAL_PENDING => (StatusCodes.REGISTRATION_APPROVAL_PENDING, None)
            case _ => (StatusCodes.LOGIN, Some(sessionId))
        }
    }

}
