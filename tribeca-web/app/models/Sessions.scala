package models

import models.database._
import play.api.Play.current
import play.api.db.slick.Config.driver.simple._
import scala.slick.session.Session

import slick.driver.ExtendedProfile
import play.api.db.slick.Profile
import play.api.db.slick.DB
import models.database.Session

object Sessions {
  var table = database.Sessions

  def getSession(sessionId: String): Query[database.Sessions.type , database.Session] = DB.withSession{implicit session: scala.slick.session.Session =>
      return for { se <- table if se.Id === sessionId} yield se
  }

  //crud
  def createSession(uid: Long): String = DB.withSession{implicit session: scala.slick.session.Session =>
    val sessionId = createSessionId
    val successful = table.insert(Session(sessionId,uid,"",System.currentTimeMillis/1000))
    if(successful == 1){
      return sessionId
    }else{
      return null
    }
  }

  def readSession(sessionId: String): database.Session = DB.withSession{implicit session: scala.slick.session.Session =>
    val userSesssions = getSession(sessionId).list
    if(userSesssions.length > 0 ){
      return userSesssions.last
    } else{
      return null
    }
  }

  def updateSession(userSession: models.database.Session) = DB.withSession{implicit session: scala.slick.session.Session =>
    val userSes = getSession(userSession.Id)
    userSes.update(userSession)
  }

  def deleteSession(sessionId: String) = DB.withSession{implicit session: scala.slick.session.Session =>
    val userSes = getSession(sessionId)
    userSes.delete
  }



  def createSessionId(): String = {
    java.util.UUID.randomUUID().toString()
  }
}
