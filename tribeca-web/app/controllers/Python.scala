package controllers

import play.api.mvc._
import controllers.Session._
import services.authorize.CookieGenerator

object Python extends Controller{
  var ipython = Authenticated{ request =>

    val url = request.user._1.ipythonUrl.toString
    Ok(views.html.ipython("Ipython", request.user._1)).withCookies(new CookieGenerator createCookie("secret", url))
  }
}
