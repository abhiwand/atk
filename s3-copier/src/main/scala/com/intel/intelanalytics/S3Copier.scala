//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

import awscala._, s3._
import scalaz._
import Scalaz._
import scala.concurrent.duration._

package com.intel.intelanalytics {

import awscala.sqs.{Queue, Message, SQS}
import scalax.io.StandardOpenOption

//TODO: make this app work using distcp instead
//import org.apache.hadoop.tools.{DistCpOptions, DistCp}

import play.api.libs.json.Json
import org.apache.hadoop.conf.Configuration
import scala.collection._
import scalax.file.Path
import org.apache.hadoop.fs.{FileSystem, Path => HdPath}
import scala.concurrent._
import ExecutionContext.Implicits.global



/**
 * Configuration for the S3Copier application
 */
case class Config(bucket: String = "gaopublic",
                  prefix: String = "invalid",
                  destination: String = "/user/gao", queue: String = "gao",
                  statusDestination: String = "/tmp/s3-copier", loopDelay: Int = 1000)

/**
 * Companion object for Config class
 */
object Config {
  /**
   * Parse command line options
   */
  def parse(args: Array[String]): Config = {
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
}

object Main {
  /**
   * Reads command arguments, watches an SQS queue for messages, dispatches them for processing
   */
  def main(args: Array[String]) {

    // It appears to be impossible to get through Intel's proxywall with this.
    // I've also tried -D arguments as well as environment variables, even all three
    // together, nothing worked. So you'll have to do testing in AWS.

    //    val config = new ClientConfiguration()
    //                      .withProxyHost("proxy.jf.intel.com")
    //                      .withProxyPort(912)
    //                      .withProtocol(Protocol.HTTPS)

    val config = Config.parse(args)

    println("Creating S3 object")
    implicit val s3 = S3().at(Region.US_WEST_2)

    implicit val sqs = SQS().at(Region.US_WEST_2)
    println("Getting/creating queue")
    val queue = sqs.queue(config.queue) getOrElse sqs.createQueue(config.queue)
    val configuration = new Configuration()
    val fs = FileSystem.get(configuration)
    val copier = new S3Copier(queue, sqs, s3, config, fs)
    copier.run()
  }
}

/**
 * Status information
 * @param name the name that should appear for the user who is tracking this status
 * @param progress the progress toward completion. This should be a number between 0 and 100.
 */
case class Status(name: String, progress: Float)

/**
 * Watches SQS for 'create' messages, copies the file named therein to HDFS.
 * Generates .status files containing status information about the transfers that are
 * in progress, so that other applications can report status information to the user
 */
class S3Copier(queue: Queue, implicit val sqs: SQS, implicit val s3: S3, config: Config, fs: FileSystem) {

  val inProgress = mutable.Map[String,Future[Status]]()
  /**
   * Stub for later connecting with a proper logging system
   */
  //TODO: event logging
  def log[T](message: String): Option[T] = {
    println(message)
    None
  }

  /**
   * Generate a JSON file representing the given status
   *
   * @param progressFolder the folder where the file should be written
   * @param status the status object to serialize to JSON
   */
  def writeProgress(progressFolder: String, status: Status) = {
    implicit val StatusFormat = Json.format[Status]
    val json = Json.toJson(status)
    val path = Path.fromString(progressFolder)

    if (!path.exists) {
      path.createDirectory(createParents = true)
    }
    (path /(s"${status.name}.status", '/')).write(Json.stringify(json))
  }



  /**
   * The processing loop. Watches an SQS queue for messages, dispatches them for processing
   */
  def run() = {
    while (true) {
      try {
        queue.messages().foreach {
          msg => processMessage(msg)
        }
        Thread.sleep(config.loopDelay)
        if (!inProgress.isEmpty) {
          log("Final results:")
          inProgress.foreach(kv =>
            try {
              log(Await.result(kv._2, Duration.Inf).toString)
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
  }

  /**
   * Parses the message, and if it is valid and appropriate for this configuration,
   * copies the file from S3 to HDFS.
   *
   * @param msg the SQS message
   */
  def processMessage(msg: Message) {
    log(msg.body)
    val json = Json.parse(msg.body)

    val file = for {
      bucketName <- (json \ "create" \ "bucket").asOpt[String] orElse log("bucket not found in message")
      validBucket <- (bucketName == config.bucket).option(bucketName) orElse log(s"bucket name $bucketName does not match ${config.bucket}")
      fileName <- (json \ "create" \ "path").asOpt[String] orElse log("path not found in message")
      valid <- fileName.startsWith(config.prefix).option(fileName) orElse log(s"fileName $fileName does not match prefix ${config.prefix}")
      bucket <- s3.bucket(bucketName) orElse log(s"bucket $bucketName does not exist")
      file <- bucket.get(fileName) orElse log(s"uploaded file $fileName does not exist")
    } yield file

    log(file.toString)

    file.foreach {
      f =>
        val result: Future[Status] = copyFile(f, config, fs)
        inProgress.put(f.key, result)
        msg.destroy()
    }
  }

  /**
   * Copies the given file from S3 to HDFS, and generates a status file
   * that it updates during the process.
   * @param s3Object the file to copy
   * @param config the copier configuration
   * @param fs the hdfs filesystem to use for copying
   */
  def copyFile(s3Object: S3Object, config: Config, fs: FileSystem): Future[Status] = {
    val name = s3Object.key.substring(config.prefix.length)
    var status = Status(name, 0)
    writeProgress(config.statusDestination, status)
    val localPath = Path.fromString(config.statusDestination) / (name, '/')
    future {
      val resource = scalax.io.Resource.fromInputStream(s3Object.content)
      localPath.outputStream(StandardOpenOption.Create).doCopyFrom(resource.inputStream)
      log(s"Wrote to ${localPath.path}")
      status = status.copy(progress = 50)
      writeProgress(config.statusDestination, status)
      log(s"Local exists: ${localPath.exists}")

      fs.copyFromLocalFile(new HdPath("file://" + localPath.path), new HdPath(config.destination + "/" + name))
      localPath.delete(force = true)
      log(s"Wrote to HDFS: ${config.destination}")
      status = status.copy(progress = 100)
      writeProgress(config.statusDestination, status)
      status
    }

    //TODO: Use DistCp instead. Currently doesn't work, always get 403 errors
    //from S3, no matter how lax we make the S3 permissions.
    //We'll use job progress from distcp to update the status file, that will give
    //better granularity of progress updates.

    //    val source = s3Object.publicUrl.toString
    //    val destination = new HdPath(config.destination + "/" + s3Object.key)
    //    val distCp = new DistCp(configuration, new DistCpOptions(new HdPath(source), destination))
    //    log("Starting distcp")
    //    val job = distCp.execute()
    //    val result = future {
    //      while (!job.isComplete) {
    //        status = status.copy(currentSize = fs.getContentSummary(destination).getLength)
    //        log(s"updated progress for ${s3Object.key} : ${status.progress()}")
    //        writeProgress(config.statusDestination, status)
    //        Thread.sleep(1000)
    //      }
    //      if (job.isSuccessful) {
    //        log(s"Message processed: ${s3Object.key}")
    //      } else {
    //        log("Failed to copy ${s3Object.key}")
    //      }
    //      status
    //    }
    //result
  }
}

}


