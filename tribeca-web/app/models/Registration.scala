package models

import play.api.libs.json.{Json, JsValue}
import play.api.data._
import play.api.data.Forms._
import play.api.Play.current
import play.api.db.slick.Config.driver.simple._
import play.api.db.slick.DB
import models.database.{Registration, Session}


case class RegistrationFormMapping(name:String,organization_name: String, organization_phone: String,
                        organization_email: String,experience:Int, role:String, whyParticipate:String,
                        whatTools:String, terms: String, authResult: String)

object Registrations{
  val RegistrationFormValidation = Form(
    mapping(
      "name" -> nonEmptyText,
      "organization_name" -> nonEmptyText,
      "organization_phone" -> nonEmptyText,
      "organization_email" -> email,
      "experience" -> number,
      "role" ->nonEmptyText,
      "whyParticipate" -> nonEmptyText,
      "whatTools" -> nonEmptyText,
      "terms" -> nonEmptyText,
      "authResult" -> nonEmptyText
    )(RegistrationFormMapping.apply)(RegistrationFormMapping.unapply)
  )

  //crud
  def createRegistration(registration:database.Registration): Long = DB.withSession{implicit session: scala.slick.session.Session =>
    database.Registrations.insert(registration)
  }
}