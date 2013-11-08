package services.aws

import com.amazonaws.services.sqs.model._
import com.amazonaws.services.sqs.{AmazonSQSClient, AmazonSQS}
import java.util

import collection.JavaConversions._
import play.api.libs.json.Json
import scala.collection.mutable.ArrayBuffer
import services.{awsUser, aws}
import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}

//import com.amazonaws.services.sns.model.AddPermissionRequest

import play.api.Play
import play.api.Play.current
import scala.collection.JavaConversions._

object SQS {
  var sqs = new AmazonSQSClient(baseCredentials)
  sqs.setEndpoint(Play.application.configuration.getString("aws.SQS.endPoint").get)

  def getQueueAttributes(url:String):scala.collection.mutable.Map[String,String] ={
    val queueAttributeRequest = new GetQueueAttributesRequest(url)
    queueAttributeRequest.setAttributeNames(ArrayBuffer("All"))

    val queueAttributes = sqs.getQueueAttributes(queueAttributeRequest)
    mapAsScalaMap(queueAttributes.getAttributes)
  }

  def createPermissionPolicy(queueName:String, url: String): String ={
    val pi = collection.mutable.Seq()
    val principalIds = Play.application.configuration.getStringList("aws.SQS.principalIds").get
    val permissions = Play.application.configuration.getStringList("aws.SQS.permissions").get

    val attributes = getQueueAttributes(url)
    val queueArn = attributes.get("QueueArn").get
    collection.immutable.Seq(pi:_*)
    val Statement = Json.arr(
      Json.obj(
        "Sid" -> ("Sid-" + queueName),
        "Effect" -> "Allow",
        "Principal" -> Json.obj("AWS" -> principalIds.toList ),
        "Action" -> permissions.toList,
        "Resource" -> queueArn
      )
    )
    val policy = Json.obj("Version" -> Play.application.configuration.getString("aws.SQS.policyVersion").get,
                          "Id" -> (queueArn + "/SQSDefaultPolicy"),
                          "Statement" -> Statement
                          )
    Json.stringify(policy)
  }

  def createQueue(clusterId: String): String ={
    val queue = new CreateQueueRequest(clusterId)
    val queueUrl = sqs.createQueue(queue)

    val policyMap =  Map( "Policy" -> createPermissionPolicy(clusterId, queueUrl.getQueueUrl))
    val settigns = new SetQueueAttributesRequest(queueUrl.getQueueUrl, mapAsJavaMap(policyMap))
    sqs.setQueueAttributes(settigns)

    queueUrl.getQueueUrl
  }

  def setMessage(url:String, message:String){
    val messageRequest = new SendMessageRequest(url, message)
    sqs.sendMessage(messageRequest)
  }

  def setFileCreateMessage(fileName:String, size:Long){

  }
}
