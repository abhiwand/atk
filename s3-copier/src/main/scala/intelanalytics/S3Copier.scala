import awscala._, s3._
import scalaz._
import Scalaz._
import scala.concurrent.duration._

package com.intel.intelanalytics {

import awscala.sqs.{Message, SQS}
import scalax.io.{Input, StandardOpenOption}

//import org.apache.hadoop.tools.{DistCpOptions, DistCp}

import play.api.libs.json.Json
import org.apache.hadoop.conf.Configuration
import scala.collection._
import scalax.file.Path
import org.apache.hadoop.fs.{FileSystem, Path => HdPath}
import scala.concurrent._
import ExecutionContext.Implicits.global

case class FileStatus(file: String, currentSize: Long, finalSize: Long) {
  def progress(): Int = {
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
    val path = Path.fromString(progressFolder)

    if (!path.exists) {
      path.createDirectory(createParents = true)
    }
    (path /(s"${status.file}.status", '/')).write(Json.stringify(json))
  }

  def parseConfig(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config]("s3-copier") {
      head("s3-copier", "1.x")
      arg[String]("bucket") action {
        (x, c) => c.copy(bucket = x)
      } text "S3 bucket to watch"
      arg[String]("prefix") action {
        (x, c) => c.copy(prefix = x)
      } text "prefix to limit which files can be copied"
      arg[String]("destination") action {
        (x, c) => c.copy(destination = x)
      } text "hdfs location to which files should be copied"
      arg[String]("queue") action {
        (x, c) => c.copy(queue = x)
      } text "queue to watch for upload messages"
      arg[String]("statusDestination") action {
        (x, c) => c.copy(statusDestination = x)
      } text "directory where status files should be placed"
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
    val inProgress = mutable.Map[String, Future[FileStatus]]()
    val configuration = new Configuration()
    val fs = FileSystem.get(configuration)
    while (true) {
      try {
        queue.messages().foreach {
          msg => processMessage(msg, config, s3, sqs, configuration, fs, inProgress)
        }
        Thread.sleep(1000);
        if (!inProgress.isEmpty) {
          log("Final results:")
          inProgress.foreach(kv =>
            try {
              println(Await.result(kv._2, Duration.Inf).toString)
            } catch {
              case e: Exception => log(e.toString)
            })
        }
        inProgress.clear()
      }
      catch {
        case e: Exception => log(e.toString)
      }
    }



    //    log(s"Files in bucket ${config.bucket} starting with ${config.prefix}:")
    //    s3.bucket(config.bucket).get.keys()
    //        .filter(_.startsWith(config.prefix))
    //        .foreach(log)
  }


  def processMessage(msg: Message, config: Config, s3: S3,
                     sqs: SQS,
                     configuration: Configuration,
                     fs: FileSystem, inProgress: mutable.Map[String, Future[FileStatus]]) {
    log(msg.body)
    val json = Json.parse(msg.body)
    implicit val s3impl = s3
    implicit val sqsImpl = sqs

    val file = for {
      bucketName <- (json \ "create" \ "bucket").asOpt[String] orElse log("bucket not found in message")
      validBucket <- (bucketName == config.bucket).option(bucketName) orElse log(s"bucket name ${bucketName} does not match ${config.bucket}")
      fileName <- (json \ "create" \ "path").asOpt[String] orElse log("path not found in message")
      valid <- fileName.startsWith(config.prefix).option(fileName) orElse log(s"fileName ${fileName} does not match prefix ${config.prefix}")
      bucket <- s3.bucket(bucketName) orElse log(s"bucket $bucketName does not exist")
      file <- bucket.get(fileName) orElse log(s"uploaded file $fileName does not exist")
    } yield file

    log(file.toString)

    file.foreach {
      f =>
        val result: Future[FileStatus] = copyFile(f, config, configuration, fs)
        inProgress.put(f.key, result)
    }
    msg.destroy()
  }

  def copyFile(f: S3Object, config: Config, configuration: Configuration, fs: FileSystem): Future[FileStatus] = {
    val len = f.getObjectMetadata.getContentLength
    var status = FileStatus(f.key, 0, len)
    writeProgress(config.statusDestination, status)
    val localPath = Path.fromString(config.statusDestination) /(f.key, '/')
    future {
      val resource = scalax.io.Resource.fromInputStream(f.content)
      localPath.outputStream(StandardOpenOption.Create).doCopyFrom(resource.inputStream)
      log(s"Wrote to $localPath")
      log(s"Local exists: ${localPath.exists}")
      val fs = FileSystem.get(configuration)

      fs.copyFromLocalFile(new HdPath("file://" + localPath.path), new HdPath(config.destination + "/" + f.key.split('/').last))
      log(s"Wrote to HDFS: ${config.destination}")
      status = status.copy(currentSize = len)
      writeProgress(config.statusDestination, status)
      status
    }


    //    val source = f.publicUrl.toString
    //    val destination = new HdPath(config.destination + "/" + f.key)
    //    val distCp = new DistCp(configuration, new DistCpOptions(new HdPath(source), destination))
    //    log("Starting distcp")
    //    val job = distCp.execute()
    //    val result = future {
    //      while (!job.isComplete) {
    //        status = status.copy(currentSize = fs.getContentSummary(destination).getLength)
    //        log(s"updated progress for ${f.key} : ${status.progress()}")
    //        writeProgress(config.statusDestination, status)
    //        Thread.sleep(1000)
    //      }
    //      if (job.isSuccessful) {
    //        log(s"Message processed: ${f.key}")
    //      } else {
    //        log("Failed to copy ${f.key}")
    //      }
    //      status
    //    }
    //result
  }
}

}


