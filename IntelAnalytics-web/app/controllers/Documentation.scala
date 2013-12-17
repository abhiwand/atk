package controllers

import play.api.mvc._
import controllers.Session._

object Documentation extends Controller{

  /**
   * main documentation page
   */
  def documentation = Authenticated{ request =>
    Ok(views.html.documen.index("API Documentation", request.user.userInfo))
  }

  def pythonApi = Authenticated{ request =>
    Ok(views.html.documen.pythonApi("Python API", request.user.userInfo));
  }

  def pythonPigApi = Authenticated{ request =>
    Ok(views.html.documen.pythonPigApi("Python Pig API", request.user.userInfo))
  }

  def javaApi = Authenticated{ request =>
    Ok(views.html.documen.javaApi("Java API", request.user.userInfo))
  }

  def javaPigApi = Authenticated{ request =>
    Ok(views.html.documen.javaPigApi("Java Pig API", request.user.userInfo))
  }

}
