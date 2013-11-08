package controllers

import controllers.Session._
import services.aws.SQS

object File {

  val create = Authenticated(parse.json){ request =>
    val queueUrl = SQS.createQueue(request.user._1.clusterId.getOrElse("waitingForClusterId"))
    Ok("")
  }

  val delete = Authenticated(parse.json){ request =>
    Ok("")
  }
}
