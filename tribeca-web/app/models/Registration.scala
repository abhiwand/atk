package models

import play.api.libs.json.{Json, JsValue}
import play.api.data._
import play.api.data.Forms._


case class Registration(myname:String,companyname: String, phone_us: String,
                        email: String,experience:Int, role:String, whyParticipate:String,
                        whatTools:String, terms: String, authResult: String)

object Registration{
  val RegistrationForm = Form(
    mapping(
      "myname" -> nonEmptyText,
      "companyname" -> nonEmptyText,
      "phone_us" -> nonEmptyText,
      "email" -> email,
      "experience" -> number,
      "role" ->nonEmptyText,
      "whyParticipate" -> nonEmptyText,
      "whatTools" -> nonEmptyText,
      "terms" -> nonEmptyText,
      "authResult" -> nonEmptyText
    )(Registration.apply)(Registration.unapply)
  )
}