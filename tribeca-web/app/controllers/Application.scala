package controllers

import play.api._
import play.api.mvc._
import play.libs.Json

object Application extends Controller {

  def index = Action {
    Ok(views.html.index("Index"))
  }



}