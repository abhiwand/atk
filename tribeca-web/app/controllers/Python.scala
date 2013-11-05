package controllers

import play.api.mvc._
import controllers.Session._
import services.authorize.CookieGenerator

object Python extends Controller{
  var ipython = Authenticated{ request =>
    implicit val userIdentifier = request.user._1.email
    Ok(views.html.ipython("Ipython", request.user._1)).withCookies(CookieGenerator.createCookie("secret", "username-{host}-{port}"))
  }
}
