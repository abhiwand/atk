package controllers

import play.api._
import play.api.mvc._
import play.libs.Json


object LoginController extends Controller {

  def login(jsonData : String) = Action {

    val data = AuthController.getUserDataFromGoogle(jsonData)
    //use the token to call google api
    //use the data from google api
    Ok(views.html.index("Index"))
  }



}
