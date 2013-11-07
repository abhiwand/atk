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
import models.database.{LoginOutput, StatementGenerator, MySQLStatementGenerator}
import models._
import controllers.Session._
import models.StatusCodes
import play.api.mvc.Cookie
import scala.Some
import play.api.mvc.SimpleResult

/**
 * Singleton object to handle log in request and generate response accordingly.
 */
object Login extends Controller {

    var simpleResult: SimpleResult = Ok

    var login = Action(parse.json) {
        request => {
            val auth = new Authorize(request.body, Providers.GooglePlus)
            val response = getResponse(auth, Sessions, MySQLStatementGenerator)
            response._1 match {
                case StatusCodes.LOGIN => Ok(StatusCodes.getJsonStatusCode(StatusCodes.LOGIN)).withNewSession.withSession(SessionValName -> response._2.get)
                case StatusCodes.FAIL_TO_VALIDATE_AUTH_DATA => Redirect("/").withCookies(Cookie("authenticationFailed","true", Some(3600),
                    "/", None, true, false ))

                case _ => Ok(StatusCodes.getJsonStatusCode(response._1))
            }
        }
    }

    /**
     *
     * @param Authorization info
     * @return tuple of (status code, session Id)
     */
    def getResponse(auth: Authorize, sessionGen: SessionGenerator, statementGenerator: StatementGenerator): (Int, Option[String]) = {

        if (auth.validateUserInfo() == None)
            return (StatusCodes.FAIL_TO_VALIDATE_AUTH_DATA, None)

        val result = Users.login(auth.userInfo.get.email, statementGenerator)

        getResponseFromLoginResult(result, sessionGen)
    }

    def getResponseFromLoginResult(result: LoginOutput, sessionGen: SessionGenerator): (Int, Option[String]) = {
        if (result.success == 1)
            (StatusCodes.LOGIN, Some(sessionGen.create(result.uid)))
        else
            (result.errorCode, None)
    }
}
