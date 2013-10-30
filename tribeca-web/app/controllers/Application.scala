package controllers

import play.api.mvc._
import play.api.libs.json.Json
import controllers.Session.ActionWithSession

//import play.api.libs.json.Json

object Application extends Controller {

  def index() = ActionWithSession { request =>
    Ok(views.html.index("Index", request.user._1))
  }

  var approvalPending = ActionWithSession(request =>
    Ok("approvalpending")
  )

  var logout = Action{
    Redirect("/").withNewSession
  }
  def authenticate = Action{

    Ok(Json.obj("user"-> "test"))
  }

}