package models

import play.api.libs.json.{JsObject, Json}
import play.api.mvc.SimpleResult

package object StatusCodes {
    val ALREADY_REGISTER = 1002
    val REGISTRATION_APPROVAL_PENDING = 1003
    val NOT_YET_REGISTERED = 1004
    val REGISTERED = 1000
    val LOGIN = 1001

  val SC = Map(
    1001 -> "ipython",
    1000 -> "ipython",
    1002 -> "ipython",
    1003 -> "",
    1004 -> ""
  )

  def getJsonStatusCode(code: Int): JsObject  = {
    Json.obj("code" -> code.toString, "url" -> SC.get(code) )
  }
}