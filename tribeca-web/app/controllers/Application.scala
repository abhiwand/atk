package controllers

import play.api.mvc._
import controllers.Session.ActionWithSession

//import play.api.libs.json.Json

object Application extends Controller {

  def index() = ActionWithSession { request =>
    Ok(views.html.index("Index", request.user._1))
  }

  var logout = Action{
    Redirect("/").withNewSession
  }
}