package controllers

import play.api.mvc._
import scala.concurrent.Future
import models.database.{WhiteList, User}
import models.Users
import models.Whitelists


object Session extends Controller{

  val SessionValName = "SESSIONID"
  val SessionTimeout = 3600

  def createSessionId(): String = {
    java.util.UUID.randomUUID().toString()
  }

  def validateSessionId(sessionId: String):  (Boolean, models.database.Session) = {
    val userSession = models.Sessions.readSession(sessionId)
    if( System.currentTimeMillis/1000 - userSession.timestamp > SessionTimeout ){
      return (false,null)
    } else{
      return (true,userSession)
    }
  }


  class AuthenticatedRequest[A](val user : (User,WhiteList), request: Request[A]) extends WrappedRequest[A](request)
  class ActionWithSession[A](val user : (User,WhiteList), request: Request[A]) extends WrappedRequest[A](request)

  object ActionWithSession extends ActionBuilder[ActionWithSession] {
    def invokeBlock[A](request: Request[A], block: (ActionWithSession[A]) => Future[SimpleResult]) = {
      request.session.get(SessionValName).map { sessionId =>
        //validate session id
        val validatedSession = validateSessionId(sessionId)
        if(validatedSession._1){
          //get user info
          val u = Users.readUser(validatedSession._2.uid, models.database.Users, models.database.WhiteLists)
          //continue with the request
          block(new ActionWithSession(u, request))
        } else{
          block(new ActionWithSession((Users.anonymousUser(),Whitelists.anonymousWhitelist()), request))
        }
      } getOrElse {
        block(new ActionWithSession((Users.anonymousUser(),Whitelists.anonymousWhitelist()), request))
      }
    }
  }

  object Authenticated extends ActionBuilder[AuthenticatedRequest] {
    def invokeBlock[A](request: Request[A], block: (AuthenticatedRequest[A]) => Future[SimpleResult]) = {
      request.session.get(SessionValName).map { sessionId =>
      //validate session id
        val validatedSession = validateSessionId(sessionId)
        if(validatedSession._1){
          //get user info
          val u = Users.readUser(validatedSession._2.uid, models.database.Users, models.database.WhiteLists)
          //continue with the request
          if(u._2.email.isEmpty || u._2.uid.get == 0){
            Future.successful(Redirect("approvalpending"))
          } else{
            block(new AuthenticatedRequest(u, request))
          }
        } else{
          Future.successful(Redirect("/"))
        }
      } getOrElse {
        Future.successful(Redirect("/"))
      }
    }
  }



}
