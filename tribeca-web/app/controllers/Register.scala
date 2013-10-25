package controllers

import play.api.mvc._
import play.api.libs.json._
import com.fasterxml.jackson.databind.JsonNode
import play.api.libs.functional.syntax._
import play.api.libs.ws.WS
import scala.concurrent.Await
import play.api.libs.concurrent.Execution.Implicits._

object Register extends Controller{
  implicit val rds = (
      (__ \ '_aa).read[String] and
      (__ \ 'access_token).read[String] and
      (__ \ 'authuser).read[String] and
      (__ \ 'client_id).read[String] and
      (__ \ 'code).read[String] and
      (__ \ 'cookie_policy).read[String] and
      (__ \ 'expires_at).read[String] and
      (__ \ 'expires_in).read[String] and
      (__ \ 'g_user_cookie_policy).read[String] and
      (__ \ 'id_token).read[String] and
      (__ \ 'issued_at).read[String] and
      (__ \ 'prompt).read[String] and
      (__ \ 'response_type).read[String] and
      (__ \ 'scope).read[String] and
      (__ \ 'session_state).read[String] and
      (__ \ 'state).read[String] and
      (__ \ 'token_type).read[String]
    )tupled

  implicit val fjs = (
      (__ \ 'issued_to).read[String] and
      (__ \ 'audience).read[String] and
      (__ \ 'user_id).read[String] and
      (__ \ 'scope).read[String] and
      (__ \ 'expires_in).read[String] and
      (__ \ 'email).read[String] and
      (__ \ 'verified_email).read[String] and
      (__ \ 'access_type).read
    )tupled

  val clientId = "141308260505-jf332k2mi49jggi2cugf08vk17u9s9rk.apps.googleusercontent.com"
  val clientSecret = "0fp9P9isYAz_vrlyA9I1Jk_j"
  val tokenVerify = "https://www.googleapis.com/oauth2/v1/tokeninfo"
  var register = Action(parse.json){ request =>
       request.body.validate(rds).map{
         case(aa, access_token, authUser, client_id, code, cookie_policy, expires_at, expires_in, g_user_cookie_policy,
           id_token, issued_at, prompt, response_type, scope, session_state, state, token_type) =>

           //val response = WS.url(tokenVerify).withQueryString("access_token", access_token).get();
           val responseFuture = WS.url(tokenVerify).withQueryString("access_token"-> access_token).get.map{ response =>
               response.status match {
                 case 200 =>
                  val userData = Json.parse(response.body).validate(fjs).map{
                    case( issued_to, audience, user_id, scope, expires_in, email, verified_email, access_type ) =>
                     audience
                  }
                 case status =>
               }

           }


/*         responseFuture.map{ response =>

           val userData = Json.parse(response.body)
           userData
         }*/



           Ok(Json.obj("_access_token" -> access_token))
       }.recoverTotal{
          e => BadRequest("bad 123 request")
       }


     }

}