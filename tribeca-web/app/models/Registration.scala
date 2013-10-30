package models

import play.api.libs.json.{Json, JsValue}
import play.api.data._
import play.api.data.Forms._

case class Registration(name: String, email: String, terms: String, phone_us: String, authResult: String)

object Registration{
  val RegistrationForm = Form(
    mapping(
      "name" -> nonEmptyText,
      "email" -> email,
      "terms" -> nonEmptyText,
      "phone_us" -> nonEmptyText,
      "authResult" -> nonEmptyText
    )(Registration.apply)(Registration.unapply)
  )
}