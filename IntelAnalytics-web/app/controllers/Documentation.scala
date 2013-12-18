package controllers

import play.api.mvc._
import controllers.Session._
import play.api.Play
import play.api.Play.current

object Documentation extends Controller{

  /**
   * main documentation page
   */
  def index = Authenticated{ request =>
    Ok(views.html.documen.index("API Documentation", request.user.userInfo))
  }

  def pythonApi = Authenticated{ request =>
    Ok(views.html.documen.pythonApi("Python API", request.user.userInfo,
      Play.application.configuration.getString("docs.python").getOrElse("")));
  }

  def pythonPigApi = Authenticated{ request =>
    Ok(views.html.documen.pythonPigApi("Python Pig API", request.user.userInfo,
      Play.application.configuration.getString("docs.pythonPig").getOrElse("")))
  }

  def javaApi = Authenticated{ request =>
    Ok(views.html.documen.javaApi("Java API", request.user.userInfo,
      Play.application.configuration.getString("docs.java").getOrElse("")))
  }

  def javaPigApi = Authenticated{ request =>
    Ok(views.html.documen.javaPigApi("Java Pig API", request.user.userInfo,
      Play.application.configuration.getString("docs.javaPig").getOrElse("")))
  }

}
