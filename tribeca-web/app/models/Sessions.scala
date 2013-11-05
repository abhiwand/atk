package models

import play.api.Play.current
import play.api.db.slick.Config.driver.simple._
import play.api.db.slick.DB
import models.database.Session

object Sessions {
  var table = database.Sessions

  def getById(sessionId: String): Query[database.Sessions.type , database.Session] = DB.withSession{implicit session: scala.slick.session.Session =>
      return for { se <- table if se.Id === sessionId} yield se
  }

  //crud
  def create(uid: Long): String = DB.withSession{implicit session: scala.slick.session.Session =>
    val sessionId = createSessionId
    val successful = table.insert(Session(sessionId,uid,"",System.currentTimeMillis/1000))
    if(successful == 1){
      return sessionId
    }else{
      return null
    }
  }

  def read(sessionId: String): database.Session = DB.withSession{implicit session: scala.slick.session.Session =>
    val userSesssions = getById(sessionId).list
    if(userSesssions.length > 0 ){
      return userSesssions.last
    } else{
      return null
    }
  }

  def update(userSession: models.database.Session) = DB.withSession{implicit session: scala.slick.session.Session =>
    val userSes = getById(userSession.Id)
    userSes.update(userSession)
  }

  def delete(sessionId: String) = DB.withSession{implicit session: scala.slick.session.Session =>
    val userSes = getById(sessionId)
    userSes.delete
  }



  def createSessionId(): String = {
    java.util.UUID.randomUUID().toString()
  }
}
