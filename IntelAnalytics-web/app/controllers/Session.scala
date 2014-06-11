//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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
import scala.concurrent.Future
import models.database.{UserDetails, DBGetUserDetailsCommand, WhiteListRow, UserRow}
import models.Users
import models.Whitelists
import play.api.Play
import play.api.Play.current

/**
 * Singleton object to provide session related services.
 */
object Session extends Controller {
    val SessionValName = Play.application.configuration.getString("session.cookie.name").get
    //"SESSIONID"
    val SessionTimeout = Play.application.configuration.getLong("session.cookie.timeout").get
    //3600
    val millisecondsPerSecond = 1000

    /**
     * Create random session id
     * @return session id
     */
    def createSessionId(): String = {
        java.util.UUID.randomUUID().toString()
    }

    /**
     * Validate the session. Check whether the session has expired or not.
     * @param sessionId
     * @return Session object
     */
    def validateSessionId(sessionId: String): Option[models.database.SessionRow] = {
        var userSession = models.Sessions.read(sessionId)
        if (userSession == None)
            None

        if (System.currentTimeMillis / millisecondsPerSecond - userSession.get.timestamp > SessionTimeout) {
            None
        } else {
            userSession.get.timestamp = System.currentTimeMillis / 1000
            //update the session timeout
            models.Sessions.update(userSession.get)
            userSession
        }
    }


    class AuthenticatedRequest[A](val user: UserDetails, request: Request[A]) extends WrappedRequest[A](request)

    class ActionWithSession[A](val user: UserDetails, request: Request[A]) extends WrappedRequest[A](request)

    object ActionWithSession extends ActionBuilder[ActionWithSession] {
        def invokeBlock[A](request: Request[A], block: (ActionWithSession[A]) => Future[SimpleResult]) = {
            request.session.get(SessionValName).map {
                sessionId =>
                //validate session id
                    val validatedSession = validateSessionId(sessionId)
                    if (validatedSession != None) {
                        //get user info
                        val u = Users.readByUid(validatedSession.get.uid, DBGetUserDetailsCommand)
                        if (u == None)
                            block(new ActionWithSession(UserDetails(Users.anonymousUser(), Whitelists.anonymousWhitelist()), request))
                        else
                        //continue with the request
                            block(new ActionWithSession(u.get, request))
                    } else {
                        block(new ActionWithSession(UserDetails(Users.anonymousUser(), Whitelists.anonymousWhitelist()), request))
                    }
            } getOrElse {
                block(new ActionWithSession(UserDetails(Users.anonymousUser(), Whitelists.anonymousWhitelist()), request))
            }
        }
    }

    object Authenticated extends ActionBuilder[AuthenticatedRequest] {
        def invokeBlock[A](request: Request[A], block: (AuthenticatedRequest[A]) => Future[SimpleResult]) = {
            request.session.get(SessionValName).map {
                sessionId =>
                //validate session id
                    val validatedSession = validateSessionId(sessionId)
                    if (validatedSession != None) {
                        //get user info
                        val u = Users.readByUid(validatedSession.get.uid, DBGetUserDetailsCommand)
                        //continue with the request
                        if (u == None || u.get.whitelistEntry.email.isEmpty || u.get.whitelistEntry.uid.get == 0) {
                            Future.successful(Redirect("/"))
                        } else {
                            block(new AuthenticatedRequest(u.get, request))
                        }
                    } else {
                        Future.successful(Redirect("/"))
                    }
            } getOrElse {
                Future.successful(Redirect("/"))
            }
        }
    }

    def onlyHttps[A](action: Action[A]) = Action.async(action.parser) {
        request =>
            request.headers.get("X-Forwarded-Proto").collect {
                case "https" => action(request)
            } getOrElse {
                Future.successful(Forbidden("Only HTTPS requests allowed"))
            }
    }

}
