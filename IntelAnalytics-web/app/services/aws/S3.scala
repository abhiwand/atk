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
package services.aws

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;


import play.api.libs.json.Json
import java.util.{TimeZone, Date}
import java.text.SimpleDateFormat
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.AmazonS3Client
import java.lang.Math
import scala.collection.mutable
import sun.misc.BASE64Encoder
import play.api.Play
import play.api.Play.current
import scala.collection.JavaConversions._

object S3 {
  //one week
  val POLICY_EXPIRATION =  Play.application.configuration.getLong("aws.S3.bucket_expiration_policy").get
  val BUCKET = Play.application.configuration.getString("aws.S3.bucket").get
  val BUCKET_PREFIX = Play.application.configuration.getString("aws.S3.bucketPrefix").get
  val MAX_SIZE = Play.application.configuration.getLong("aws.S3.bucket_max_file_size").get
  val PREFIX = Play.application.configuration.getString("aws.S3.bucketUploadPrefix").get
  val BYTE = 1024
  val s3Client = new AmazonS3Client(baseCredentials);

  def formatName(key: String): String = {
    val splits = key.split("/")
    splits.last
  }

  def getBucketName: String = {
    val bucketNameParts = List[String](BUCKET_PREFIX, Play.application.mode.toString.toLowerCase,"public")
    bucketNameParts.mkString("-")
  }

  def getBucketResource:String ={
    "arn:aws:s3:::" + getBucketName
  }

  def getCORSConfig():BucketCrossOriginConfiguration ={
    val corsConfigs = Play.application.configuration.getObjectList("aws.S3.cors").get.toList map(new CR(_))

    var corsRules:List[CORSRule] = List[CORSRule]()
    for(corsConfig <- corsConfigs){
      val rule = new CORSRule()
      rule.setAllowedOrigins(corsConfig.origin)
      rule.setAllowedHeaders(corsConfig.allowedHeaders)
      rule.setExposedHeaders(corsConfig.exposedHeaders)
      rule.setAllowedMethods(corsConfig.methods)
      corsRules ::= rule
    }

    new BucketCrossOriginConfiguration(corsRules)
  }

  def getPermissions(): List[String] = {
    val permissions = Play.application.configuration.getStringList("aws.S3.permissions").get.toList
    var S3Permissions:List[String] = List[String]()
    for(permission <- permissions){
      S3Permissions ::= "S3:" + permission
    }
    S3Permissions
  }

  def getBucketPolicy: String = {
    val principalIds = Play.application.configuration.getStringList("aws.S3.principalIds").get
    val permissions = getPermissions

    val Statement = Json.arr(
      Json.obj(
        "Sid" -> ("Sid-" + getBucketName),
        "Effect" -> "Allow",
        "Principal" -> Json.obj("AWS" -> principalIds.toList ),
        "Action" -> permissions.toList,
        "Resource" -> getBucketResource
      )
    )
    val policy = Json.obj("Version" -> Play.application.configuration.getString("aws.S3.policyVersion").get,
      "Id" -> (getBucketName + "-DefaultPolicy"),
      "Statement" -> Statement
    )
    Json.stringify(policy)
  }

  def createBucket() {
    var bucket:Bucket = new Bucket()
    try{
      bucket = s3Client.createBucket(getBucketName, Region.US_West_2)
    } catch {
      case e: AmazonS3Exception =>
        if(!e.getErrorCode.equals("BucketAlreadyOwnedByYou")){
            throw e
        }
    }
    s3Client.setBucketCrossOriginConfiguration(getBucketName, getCORSConfig)

    val bucketAcl = s3Client.getBucketAcl(getBucketName)
    val grantee = new CanonicalGrantee("arn:aws:iam::953196509655:user/IntelAnalytics_Web")
    bucketAcl.grantPermission(grantee, Permission.FullControl)
    s3Client.setBucketPolicy(getBucketName, getBucketPolicy )
  }
  def formatSize(size: Long): String = {
    val sizes = Array("Bytes", "KB", "MB", "GB", "TB")
    if (size == 0) return "n/a"
    val sizeIndex = Math.floor(Math.log(size) / Math.log(BYTE)).toInt
    Math.round(size / Math.pow(BYTE, sizeIndex)).toString + " " + sizes(sizeIndex)
  }

  def getObjectList(userIdentifier: String): List[S3ObjectSummary] = {
    if(userIdentifier.isEmpty){
      List()
    }else{
      val objectList = s3Client.listObjects(getBucketName, PREFIX + userIdentifier + "/")
      //scala.collection.JavaConversions.asScalaBuffer[S3ObjectSummary](objectList.getObjectSummaries)
      objectList.getObjectSummaries.toList
    }
  }

  def uploadDirectory(userIdentifier:String): String = {
    PREFIX + userIdentifier + "/"
  }
  def createPolicy(userIdentifier: String): String = {

    val expire = new Date(System.currentTimeMillis() + POLICY_EXPIRATION)
    val dateFormat = new SimpleDateFormat("yyyy-MM-d'T'hh:mm:ss'Z'")
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    val policyJson = Json.obj("expiration" -> dateFormat.format(expire),
      "conditions" -> Json.arr(Json.obj("bucket" -> getBucketName),
        Json.arr("starts-with", "$key", uploadDirectory(userIdentifier)),
        Json.obj("acl" -> "private"),
        //Json.obj("success_action_redirect" -> "https://localhost/"),
        //Json.arr("starts-with", "$Content-Type", ""),
        Json.arr("content-length-range", 0, MAX_SIZE))
    )
    Json.stringify(policyJson)
  }

  def encodePolicy(policy_document: String): String = {
    new BASE64Encoder().encode(policy_document.getBytes("UTF-8")).replaceAll("\n", "").replaceAll("\r", "");
  }

  def getPolicy(userIdentifier: String): String = {
    val json = createPolicy(userIdentifier)
    encodePolicy(json)
  }

  def getSignature(implicit policy: String): String = {
    createSignature(policy)
  }

  def createSignature(policy: String): String = {
    val hmac = Mac.getInstance("HmacSHA1");
    hmac.init(new SecretKeySpec(baseSecretAccessKey.getBytes("UTF-8"), "HmacSHA1"));
    new BASE64Encoder().encode(hmac.doFinal(policy.getBytes("UTF-8"))).replaceAll("\n", "");
  }

  def deleteObject(key: String){
    s3Client.deleteObject(getBucketName, key)
  }
}
