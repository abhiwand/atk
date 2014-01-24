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

import util.control.Breaks._
import awscala.sqs.{Queue, Message, SQS}
import scalax.io._
import java.net.URI
import java.io.File._
import awscala.File
import com.amazonaws.auth.{EnvironmentVariableCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.event._
import com.amazonaws.services.s3.model.{GetObjectRequest}
import com.amazonaws.services.s3.transfer.internal.ProgressListenerChain
import com.amazonaws.event.ProgressListenerChain.ProgressEventFilter
import java.io._
import org.apache.commons.codec.Charsets
import scalax.io.StandardOpenOption
import com.intel.intelanalytics.Status
import awscala.sqs.Queue
import play.api.libs.iteratee.Enumerator
import org.apache.hadoop.util.Progressable
import com.intel.intelanalytics.Status
import awscala.sqs.Queue

import scalax.io.Codec
import scalax.io.JavaConverters._
import com.amazonaws.services.s3.transfer.TransferManager

//TODO: make this app work using distcp instead
//import org.apache.hadoop.tools.{DistCpOptions, DistCp}

import scala.util.{Success, Failure}
import play.api.libs.json.Json
import scala.io.Source
import org.apache.hadoop.conf.Configuration
import scala.collection._
import scalax.file.Path
import com.amazonaws._
import org.apache.hadoop.fs.{Path => HdPath, FSDataOutputStream, FileSystem}
import scala.concurrent._
import ExecutionContext.Implicits.global
import scalaj.http.Http
import scala.async.Async.{async, await}
import scala.collection.JavaConversions._
import java.nio.file._

/**
 * Configuration for the S3Copier application
 */
case class Config(bucket: String = "",
                  prefix: String = "invalid",
                  destination: String = "",
                  queue: String = "",
                  statusDestination: String = "",
                  hadoopUser: String = "hadoop",
                  hadoopURI: String = "hdfs://master:9000",
                  instanceId: String = instance.id(),
                  region: String = "us-west-2",
                  loopDelay: Int = 1000)

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
      opt[String]("destination") required() action {
        (x, c) => c.copy(destination = x)
      } text "hdfs location to which files should be copied"
      opt[String]("statusDestination") required() action {
        (x, c) => c.copy(statusDestination = x)
      } text "directory where status files should be placed"
      opt[String]("queue") optional() action {
        (x, c) => c.copy(queue = x)
      } text "queue to watch for upload messages. Defaults to instance id"
      opt[String]("prefix") optional() action {
        (x, c) => c.copy(prefix = x)
      } text "prefix to limit which files can be copied from s3. Defaults to the instance id"
      opt[String]("hadoopUser") optional() action{
        (x, c) => c.copy(hadoopUser = x)
      } text "the hadoop hdfs user. Defaults to hadoop"
      opt[String]("hadoopURI") optional() action {
        (x, c) => c.copy(hadoopURI = x)
      } text "the hadoop hdfs uri. defaults hdfs://master:9000"
      opt[String]("region") optional() action {
        (x, c) => c.copy(region = x)
      } text "the aws region for the s3 and sstatusqs client. default to us-west-2"
    }
    // parser.parse returns Option[C]
    val config = parser.parse(args, Config()) getOrElse {
      System.exit(0)
      Config()
    }

    var cConfig = config

    if( config.queue.isEmpty ){
      cConfig = cConfig.copy(queue = cConfig.instanceId)
    }
    if( config.prefix.equals("invalid")){
      cConfig = cConfig.copy(prefix = cConfig.instanceId)
    }

    cConfig
  }
}

object main {
  /**
   * Reads command arguments, watches an SQS queue for messages, dispatches them for processing
   */
  def main(args: Array[String]) {

    val config = Config.parse(args)

    println("Creating S3 object")
    //implicit val s3 = S3().at(Region.apply(config.region))
    val baseCredentials = new EnvironmentVariableCredentialsProvider().getCredentials
    implicit val javaS3Client = new AmazonS3Client(baseCredentials);
    implicit val transferManager = new TransferManager(baseCredentials);

    implicit val sqs = SQS().at(Region.apply(config.region))
    println("Getting/creating queue")
    val queue = sqs.queue(config.queue) getOrElse sqs.createQueue(config.queue)
    val configuration = new Configuration()

    var fs: FileSystem = null;
    //if you have problems getting the copier to work pass the hdfs uri and user
    if(config.hadoopURI.isEmpty || config.hadoopUser.isEmpty){
      fs = FileSystem.get(configuration)
    } else{
      val uri = new URI(config.hadoopURI);
      fs = FileSystem.get(uri, configuration, config.hadoopUser)
    }
    val copier = new S3Copier(queue, sqs, javaS3Client, transferManager, config, fs)
    copier.run()

  }
}

object instance{
  def id(): String = {
    val request: Http.Request = Http("http://169.254.169.254/latest/meta-data/instance-id")

    var instanceId: String = "invalid"
    try{
      instanceId = request.asString
      print("instance id: " + instanceId)
    } catch{
      case e: Exception => {
        print("error: coudln't get instance id " + e.getMessage + "\n")
      }
    }
    instanceId.trim.toLowerCase
  }
}

/**
 * Status information
 * @param name the name that should appear for the user who is tracking this status
 * @param progress the progress toward completion. This should be a number between 0 and 100.             import Perm._
 */
case class Status(name: String, progress: Float)

case class File(Name: String, Bucket: String)
/**
 * Watches SQS for 'create' messages, copies the file named therein to HDFS.
 * Generates .status files containing status information about the transfers that are
 * in progress, so that other applications can report status information to the user
 */
class S3Copier(queue: Queue, implicit val sqs: SQS, implicit val javaS3: AmazonS3Client, implicit val transferManager: TransferManager, config: Config, fs: FileSystem) {
  var downloading = collection.mutable.Map[String, String]()
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
  def writeProgress(progressFolder: String, status: Status, bucketName: String): String = {
    implicit val StatusFormat = Json.format[Status]
    val json = Json.toJson(status)
    val path = Path.fromString(progressFolder)

    if (!path.exists) {
      path.createDirectory(createParents = true)
    }


    (path /(s"${status.name}.status", '/')).write(Json.stringify(json))
    path.path + s"${status.name}.status"

    /* await(writeStatus) */

    /* val fileName =  s"${status.name}.status"
     val test = async{transferManager.upload(bucketName, config.prefix + fileName, new java.io.File(path.path + fileName))}


    ""*/
  }

  /**
   * The processing loop. Watches an SQS queue for messages, dispatches them for processing
   */
  def run() = {

    while (true) {
      try {
        queue.messages().foreach {
          msg => {
            if(!downloading.contains(msg.id)){
            downloading(msg.id) = msg.body
            async{
              val processed = await(processMessage(msg))
              if(processed){
                downloading remove msg.id
              }
            }
          }
          }
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
  def processMessage(msg: Message): Future[Boolean] = async{

    log(msg.body)
    val json = Json.parse(msg.body)
    val bucketName = (json \ "create" \ "bucket").asOpt[String] orElse log("bucket not found in message")
    val fileName = (json \ "create" \ "path").asOpt[String] orElse log("path not found in message")
    val fileSize = (json \ "create" \ "size").asOpt[Long] orElse log("Size not found")
    val valid = fileName.get.startsWith(config.prefix).option(fileName) orElse log(s"fileName $fileName does not match prefix ${config.prefix}")

    val progress = new ProgressListener() {
      var bytesTransferred: Long = 0l
      var total = fileSize.get
      val name = fileName.get.substring(config.prefix.length)
      val statusName = config.prefix + name + ".status"
      val reportN = Math.pow(fileSize.get * .00000005, 2)
      System.out.println(reportN)
      var reported = 0

      def status(): Double = {
        (bytesTransferred.toDouble / total.toDouble) * 100
      }
      def complete(): Boolean = {
        if(bytesTransferred >= total)
          true
        else
          false
      }

      @Override
      def progressChanged(progressEvent: ProgressEvent) {
        bytesTransferred += progressEvent.getBytesTransferred();
        reported += 1
        if (progressEvent.getEventCode() == ProgressEvent.COMPLETED_EVENT_CODE){
          System.out.print(" " + bytesTransferred + " bytes; ");
          val path = writeProgress(config.statusDestination, Status(name, 100), bucketName.get)
          val test = javaS3.putObject(bucketName.get, statusName, new java.io.File(path))
          msg.destroy
          System.out.print(test.getContentMd5)
        }
        else if(progressEvent.getEventCode == 0 ){
          val status = (bytesTransferred.toDouble / total.toDouble) * 100

          if( reported >= reportN){
            reported = 0
            System.out.println("status : " + status)
            val path = writeProgress(config.statusDestination, Status(name, status.toFloat), bucketName.get)
            val put = javaS3.putObject(bucketName.get, statusName, new java.io.File(path))
          }
        }
      }
    }

    if(!valid.get.isEmpty){

      val file = new GetObjectRequest(bucketName.get, fileName.get );


      file.setGeneralProgressListener(progress)

      val read = await(copyFile(file, config, fs))
      System.out.println("read : " + read)
      var i = 0
      while(!progress.complete()){
        if(i >= 4000000){
        System.out.println( "wait :" + progress.status())
          i = 0
        }
        i += 1
      }
    }

    if(progress.complete){
      System.out.println("destroy")

      true
    }
    else{
      false
    }
  }


  /**
   * Copies the given file from S3 to HDFS, and generates a status file
   * that it updates during the process.
   * @param s3Object the file to copy
   * @param config the copier configuration
   * @param fs the hdfs filesystem to use for copying
   */
  def copyFile(s3ObjectRequest: GetObjectRequest, config: Config, fs: FileSystem)(implicit s3Client: AmazonS3Client): Future[Long] = {
    val name = s3ObjectRequest.getKey.substring(config.prefix.length)
    val localPath = Path.fromString(config.statusDestination) / (name, '/')

    val s3Object = s3Client.getObject(s3ObjectRequest)
    s3Object.getObjectMetadata.getContentLength

    future {
      val bufferedInput = new BufferedInputStream(s3Object.getObjectContent, 5242880)

      var FSDataOutputStream: FSDataOutputStream = null;

      FSDataOutputStream = fs.create(new HdPath(config.destination + "/" + name), true )

      var read: Long = 0

      val arrayBuffer = new Array[Byte](1048576)

      var bufferRead = bufferedInput.read(arrayBuffer,0, arrayBuffer.length)

      while( bufferRead > -1 ){
	
	        //System.out.print("read: " + bufferRead + " " + " avail: " + bufferedInput.available + "\n")
	      FSDataOutputStream.write(arrayBuffer.slice(0, bufferRead))
        read = read + bufferRead
	      bufferRead = bufferedInput.read(arrayBuffer, 0, arrayBuffer.length)
      }
	
      FSDataOutputStream.close
      log(s"Wrote to ${localPath.path}")
      read
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


