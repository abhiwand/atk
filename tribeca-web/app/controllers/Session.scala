package controllers

import play.api.mvc._
import scala.concurrent.Future
import models.database.User
import models.Users


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

  class AuthenticatedRequest[A](val user : User, request: Request[A]) extends WrappedRequest[A](request)

  object Authenticated extends ActionBuilder[AuthenticatedRequest] {
    def invokeBlock[A](request: Request[A], block: (AuthenticatedRequest[A]) => Future[SimpleResult]) = {
      request.session.get(SessionValName).map { sessionId =>
        //validate session id
        val validatedSession = validateSessionId(sessionId)
        if(validatedSession._1){
          //get user info
          val u = Users.readUser(validatedSession._2.uid)
          //continue with the request
          block(new AuthenticatedRequest(u, request))
        } else{
          Future.successful(Forbidden)
        }
      } getOrElse {
        Future.successful(Forbidden)
      }
    }
  }



}
