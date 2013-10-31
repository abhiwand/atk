package controllers

import play.api.mvc._
import controllers.Session._
import services.authorize.CookieGenerator

object Python extends Controller{
  var ipython = Authenticated{ request =>

    Ok(views.html.ipython("Ipython", request.user._1)).withCookies(CookieGenerator.createCookie("secret", "username-{host}-{port}"))
  }
}
