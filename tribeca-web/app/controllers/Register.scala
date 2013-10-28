package controllers

/*import play.api._
import play.api.data._
import play.api.db.slick._
import play.api.Play.current*/
import play.api.mvc._
import play.api.libs.json._
/*import com.fasterxml.jackson.databind.JsonNode
import play.api.libs.functional.syntax._
import play.api.libs.ws.WS
import scala.concurrent.Await*/
import services.authorize.{Providers, Authorize}
//import play.api.libs.concurrent.Execution.Implicits._
import models._
import models.database.User
//import play.api.libs.json


object Register extends Controller{

  var register = Action(parse.json){ request =>
       var auth = new Authorize(request.body, Providers.GooglePlus)
       if(auth.valdiateTokenResponseData()&& auth.validateToken()){
         val userInfo = auth.getUserInfo()
         if(userInfo != null){
           //the entire session is valid and we have the user data
           var uid = Users.findByEmail(userInfo.email)
           if( uid > 0){
              //create user session
             Ok(Json.toJson("AlreadyRegistered"))
           }else{
            val u = User(None,userInfo.givenName,userInfo.familyName,userInfo.email,"Phone","company","companyemail","picture url",true,None)
            uid = Users.insert(u)
             //create user session
            Ok(Json.toJson("Registered"))
           }
           //BadRequest("could not create user")
          } else{
           BadRequest("Couldn't validate auth response data")
         }
       } else{
         BadRequest("Couldn't validate auth response data")
       }
     }

  /*def insert = DBAction{ implicit rs =>

    val us = Query(Users).list
    Ok("")

  }*/

}