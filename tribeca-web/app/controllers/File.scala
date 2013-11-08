package controllers

import controllers.Session._
import services.aws.{S3, SQS}
import play.api.libs.json.Json
import scala.util.control.Breaks._

object File {
  case class FileUpload(name:String, size:Long)
  case class FileDelete(name:String)

  implicit val fileUpload = Json.reads[FileUpload]
  implicit val fileDelete = Json.reads[FileDelete]

  val create = Authenticated(parse.json){ request =>
    request.body.validate[FileUpload](fileUpload).map{
      case(file) =>

      val queueUrl = SQS.createQueue(request.user._1.clusterId.getOrElse("waitingForClusterId"))
      val message = Json.obj("create" -> Json.obj("bucket" -> services.aws.S3.BUCKET, "path" -> (S3.uploadDirectory(request.user._1.uid.get.toString) + file.name), "size" -> file.size))
      SQS.setMessage(queueUrl, Json.stringify(message))
      Ok("")
    }.getOrElse {
      Ok("")
    }
  }

  val delete = Authenticated(parse.json){ request =>
    request.body.validate[FileDelete](fileDelete).map{
      case(file) =>
      //check files on s3
      val files = S3.getObjectList(request.user._1.uid.get.toString)
        breakable {
          for(s3Object <- S3.getObjectList(request.user._1.uid.get.toString)){
        if(s3Object.getKey.split("/").last.equals(file.name)){
          S3.deleteObject(s3Object.getKey)
          break
        }
      }}

      //send message to s3
        val queueUrl = SQS.createQueue(request.user._1.clusterId.getOrElse("waitingForClusterId"))
        val message = Json.obj("delete" -> Json.obj("bucket" -> services.aws.S3.BUCKET, "path" -> (S3.uploadDirectory(request.user._1.uid.get.toString) + file.name)))
        SQS.setMessage(queueUrl, Json.stringify(message))
      Ok("")
    }.getOrElse {
      Ok("")
    }
  }
}
