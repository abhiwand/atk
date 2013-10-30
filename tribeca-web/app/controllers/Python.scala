package controllers

import play.api.mvc._
import controllers.Session._

object Python extends Controller{
  var ipython = Authenticated{ request =>

    Ok(views.html.ipython("Ipython", request.user._1))
  }
}
