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
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.s3.AmazonS3Client
import java.lang.Math
import scala.collection.mutable
import org.apache.commons.codec.binary.Base64
import sun.misc.BASE64Encoder

object S3 {
  val aws_secret_key = "h57vzrHg18IRdGUGnRvfSph381VtuEOfK+r3oNBQ";
  val access_key = "AKIAJ65RQRJONMKNT2NQ";
  val POLICY_EXPIRATION = 604800000
  //one week in milliseconds
  val BUCKET = "gaopublic"
  val MAX_SIZE = 5368709120L
  val SUCCESS_ACTION_REDIRECT = "https://localhost/s33"
  val PREFIX = "user/"
  val BYTE = 1024

  def formatName(key: String): String = {
    val splits = key.split("/")
    splits.last
  }

  def formatSize(size: Long): String = {
    val sizes = Array("Bytes", "KB", "MB", "GB", "TB")
    if (size == 0) "n/a"
    val sizeIndex = Math.floor(Math.log(size) / Math.log(BYTE)).toInt
    Math.round(size / Math.pow(BYTE, sizeIndex)).toString + " " + sizes(sizeIndex)
  }

  def getObjectList(userIdentifier: String): mutable.Buffer[S3ObjectSummary] = {
    val myCredentials = new BasicAWSCredentials(access_key, aws_secret_key);
    val s3Client = new AmazonS3Client(myCredentials);
    val objectList = s3Client.listObjects(BUCKET, PREFIX + userIdentifier + "/")
    scala.collection.JavaConversions.asScalaBuffer[S3ObjectSummary](objectList.getObjectSummaries)
  }

  def createPolicy(userIdentifier: String): String = {

    val expire = new Date(System.currentTimeMillis() + 604800000)
    val dateFormat = new SimpleDateFormat("yyyy-MM-d'T'hh:mm:ss'Z'")
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    val policyJson = Json.obj("expiration" -> dateFormat.format(expire),
      "conditions" -> Json.arr(Json.obj("bucket" -> BUCKET),
        Json.arr("starts-with", "$key", PREFIX + userIdentifier + "/"),
        Json.obj("acl" -> "private"),
        //Json.obj("success_action_redirect" -> SUCCESS_ACTION_REDIRECT),
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
    hmac.init(new SecretKeySpec(aws_secret_key.getBytes("UTF-8"), "HmacSHA1"));
    new BASE64Encoder().encode(hmac.doFinal(policy.getBytes("UTF-8"))).replaceAll("\n", "");
  }
}
