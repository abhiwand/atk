package models

import models.database._
import play.api.Play.current
import play.api.db.slick.Config.driver.simple._
import play.api.db.slick.DB
import scala.slick.session.Session

import slick.driver.ExtendedProfile
import play.api.db.slick.Profile
import play.api.db.slick.DB

object Users{
  var table = new database.Users

  def insert(user: User): Long = DB.withSession{ implicit session: scala.slick.session.Session =>
    var uid = findByEmail(user.email)
    if(uid == 0){
      uid = table.insert(user)
    }
    return uid
  }

  val emailExist =for{
    email <- Parameters[String]
    u <- table if u.email === email
  }yield u

  def findByEmail(email: String): Long = DB.withSession {  implicit session: scala.slick.session.Session =>

    val test2 = emailExist(email).list
    if(test2.length > 0){
      return test2.last.uid.get
    }else{
      return 0
    }
  }
}
