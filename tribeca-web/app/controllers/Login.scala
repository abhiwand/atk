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
import models.database.{DBLoginCommand, StatementGenerator, MySQLStatementGenerator}
import models._
import controllers.Session._
import models.StatusCodes
import play.api.mvc.SimpleResult

/**
 * Singleton object to handle log in request and generate response accordingly.
 */
object Login extends Controller {

    abstract class LoginActionResponse
    case class SuccessfullyLoginResponse(sessionId: String) extends LoginActionResponse
    case class FailToValidateResponse() extends LoginActionResponse
    case class GeneralErrorResponse(errorCode: Int) extends LoginActionResponse

    var simpleResult: SimpleResult = Ok

    /**
     * get login result and return to user
     */
    var login = Action(parse.json) {
        request => {
            val auth = new Authorize(request.body, Providers.GooglePlus)
            getResult(auth, Sessions, MySQLStatementGenerator)
        }
    }

    /**
     * get login result from database.
     * @param auth
     * @param sessionGen
     * @param statementGenerator
     * @return
     */
    def getResult(auth: Authorize, sessionGen: SessionGenerator, statementGenerator: StatementGenerator): SimpleResult = {

        val response = getResponse(auth, sessionGen, statementGenerator)
        response match {
            case successfulResponse: SuccessfullyLoginResponse => Ok(StatusCodes.getJsonStatusCode(StatusCodes.LOGIN)).withNewSession.withSession(SessionValName -> successfulResponse.sessionId).withCookies(Register.getRegisteredCookie)
            case failedResponse: FailToValidateResponse => Ok(StatusCodes.getJsonStatusCode(StatusCodes.FAIL_TO_VALIDATE_AUTH_DATA))
            case generalErrorResponse: GeneralErrorResponse => Ok(StatusCodes.getJsonStatusCode(generalErrorResponse.errorCode))
        }
    }


    /**
     *
     * @param Authorization info
     * @return LoginActionResponse
     */
    def getResponse(auth: Authorize, sessionGen: SessionGenerator, statementGenerator: StatementGenerator): LoginActionResponse = {
        if (auth.validateToken() == None || auth.validateUserInfo() == None)
            return FailToValidateResponse()

        val result = Users.login(auth.userInfo.get.email, statementGenerator, DBLoginCommand)

        if (result.success == 1) {

            val sessionId = sessionGen.create(result.uid)
            if (sessionId == None)
                FailToValidateResponse()
            else
                SuccessfullyLoginResponse(sessionId.get)
        }
        else
            GeneralErrorResponse(result.errorCode)
    }
}


