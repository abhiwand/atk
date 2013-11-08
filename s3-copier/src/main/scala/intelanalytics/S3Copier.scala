import awscala._, s3._
package com.intel.intelanalytics {

import awscala.sqs.{Message, SQS}
import org.apache.hadoop.tools.{DistCpOptions, DistCp}
import play.api.libs.json.Json
import org.apache.hadoop.conf.Configuration
import scala.collection._
import scalax.file.Path
import org.apache.hadoop.fs.{FileSystem, Path => HdPath}
import scala.concurrent._
import ExecutionContext.Implicits.global

case class FileStatus (file: String, currentSize: Long, finalSize: Long) {
  def progress() : Int = {
    ((currentSize.toFloat / finalSize.toFloat) * 100).toInt
  }
}

case class Config(bucket: String = "gaopublic", prefix: String = "invalid", destination: String = "/user/gao", queue: String = "gao", statusDestination: String = "/tmp/s3-copier")

//TODO: LICENSE
object S3Copier {

  def log[T](message: String): Option[T] = {
    println(message)
    None
  }

  def writeProgress(progressFolder: String, status: FileStatus) = {
    implicit val fileStatusFormat = Json.format[FileStatus]
    val json = Json.toJson(status)
    (Path(progressFolder) / (status.file + ".status")).write(Json.stringify(json))
  }

  def parseConfig(args: Array[String]) : Config = {
    val parser = new scopt.OptionParser[Config]("s3-copier") {
      head("s3-copier", "1.x")
      arg[String]("bucket") action {(x,c) => c.copy(bucket = x)} text "S3 bucket to watch"
      arg[String]("prefix") action {(x,c) => c.copy(prefix = x)} text "prefix to limit which files can be copied"
      arg[String]("destination") action {(x,c) => c.copy(destination = x)} text "hdfs location to which files should be copied"
      arg[String]("queue") action {(x,c) => c.copy(queue = x)} text "queue to watch for upload messages"
      arg[String]("statusDestination") action {(x,c) => c.copy(statusDestination = x)} text "directory where status files should be placed"
    }
    // parser.parse returns Option[C]
    parser.parse(args, Config()) getOrElse {
      System.exit(0)
      Config()
    }
  }

  def main(args: Array[String]) {

    //    val config = new ClientConfiguration()
    //                      .withProxyHost("proxy.jf.intel.com")
    //                      .withProxyPort(912)
    //                      .withProtocol(Protocol.HTTPS)

    val config = parseConfig(args)

    println("Creating S3 object")
    //    val client = new ProxyS3Client(Credentials.defaultEnv, config)
    implicit val s3 = S3().at(Region.US_WEST_2)

    implicit val sqs = SQS().at(Region.US_WEST_2)
    println("Getting/creating queue")
    val queue = sqs.queue("uploads-bryn") getOrElse sqs.createQueue("uploads-bryn")
    val inProgress = mutable.Map[String,Future[FileStatus]]()
    val configuration = new Configuration()
    val fs = FileSystem.get(configuration)

    queue.messages().foreach {
      msg =>

        val json = Json.parse(msg.body)

        val file = for {
          bucketName <- (json \ "bucket").asOpt[String] orElse log("bucket not found in message")
          if bucketName == config.bucket
          fileName <- (json \ "name").asOpt[String] orElse log("fileName not found in message")
          if fileName.startsWith(config.prefix)
          bucket <- s3.bucket(bucketName) orElse log("bucket " + bucketName + " does not exist")
          file <- bucket.get(fileName) orElse log("uploaded file " + fileName + " does not exist")
        } yield file

        val result = file.map {
          f =>
            val len = f.getObjectMetadata.getContentLength
            var status = FileStatus(f.key, 0, len)
            writeProgress(config.statusDestination, status)
            val source = f.publicUrl.toString
            val destination = new HdPath(config.destination + "/" + f.key)
            val distCp = new DistCp(configuration, new DistCpOptions(new HdPath(source), destination))
            log("Starting distcp")
            val job = distCp.execute()
            val result = future {
              while(!job.isComplete) {
                status = status.copy(currentSize = fs.getContentSummary(destination).getLength)
                log("updated progress for " + f.key + ": " + status.progress())
                writeProgress(config.statusDestination, status)
                Thread.sleep(1000)
              }
              if (job.isSuccessful) {
                msg.destroy()
                log("Message processed: " + f.key)
              } else {
                log("Failed to copy " + f.key)
              }
              status
            }
            inProgress.put(f.key, result)
        }
    }
  }
}

}


