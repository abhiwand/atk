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
        //create the que
        val queueUrl = SQS.createQueue(request.user.userInfo.clusterId.getOrElse("waitingForClusterId"))
        //create the json object to send
        val message = Json.obj("create" -> Json.obj("bucket" -> services.aws.S3.BUCKET, "path" ->
          (S3.uploadDirectory(request.user.userInfo.uid.get.toString) + file.name), "size" -> file.size))
        //stringify the json to send to queue
        SQS.setMessage(queueUrl, Json.stringify(message))
        Ok(Json.obj("ok" -> ""))
    }.getOrElse {
      BadRequest
    }
  }

  val delete = Authenticated(parse.json){ request =>
    request.body.validate[FileDelete](fileDelete).map{
      case(file) =>
        //check file name  on the users dir s3
        breakable {
          for(s3Object <- S3.getObjectList(request.user.userInfo.uid.get.toString)){
            if(s3Object.getKey.split("/").last.trim.toLowerCase.equals(file.name.trim.toLowerCase)){
              S3.deleteObject(s3Object.getKey)
              break
            }
        }}

        //send message to s3
        val queueUrl = SQS.createQueue(request.user.userInfo.clusterId.getOrElse("waitingForClusterId"))
        val message = Json.obj("delete" -> Json.obj("bucket" -> services.aws.S3.BUCKET, "path" -> (S3.uploadDirectory(request.user.userInfo.uid.get.toString) + file.name)))
        SQS.setMessage(queueUrl, Json.stringify(message))
        Ok(Json.obj("file"->file.name))
    }.getOrElse {
      BadRequest
    }
  }
}
