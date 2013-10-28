package controllers

import play.api.mvc._

import play.api.libs.json._
import services.authorize.{Providers, Authorize}
import models._
import models.database.User

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
            val u = User(None,userInfo.givenName,userInfo.familyName,userInfo.email,"Phone","company","companyemail",true,None)
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



}