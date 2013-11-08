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
    //var pi = Json.arr()
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
  def test() = {
    val message = new SendMessageRequest("https://sqs.us-west-2.amazonaws.com/953196509655/SaaSFileUpdates","test two web")
    sqs.sendMessage(message)
  }

  def createQueue(clusterId: String): String ={
    val queue = new CreateQueueRequest(clusterId)
    val queueUrl = sqs.createQueue(queue)

    val policyMap =  Map( "Policy" -> createPermissionPolicy(clusterId, queueUrl.getQueueUrl))
    val settigns = new SetQueueAttributesRequest(queueUrl.getQueueUrl, mapAsJavaMap(policyMap))
    sqs.setQueueAttributes(settigns)

   /* val perm = new AddPermissionRequest(queueUrl.getQueueUrl,"addnecessaryusers",util.Arrays.asList("arn:aws:iam::953196509655:user/IntelAnalytics_Web"),
      ListBuffer(List("GetQueueAttributes"): _*))
    perm.setRequestCredentials(baseCredentials)
    val test = sqs.addPermission(perm)*/

    /*Play.application.configuration.getObjectList("aws.SQS.users").get.foreach( user =>{
      val secretAccessKey = user.toConfig.getString("secretAccessKey")
      val accessKey = user.toConfig.getString("accessKey")
      val permission = new AddPermissionRequest(queueUrl.getQueueUrl,"addPerm",util.Arrays.asList(principalId), permissions)

      permission.setRequestCredentials( new BasicAWSCredentials(secretAccessKey, accessKey))
      sqs.addPermission(permission)


      })*/

      //perm.setRequestCredentials( new BasicAWSCredentials(baseAccessKey, baseSecretAccessKey))
        //sqs.addPermission(perm)


    //Play.application.configuration.getConfigList("aws.SQS.users") map(new awsUser(_))
    //Play.application.configuration.getConfigList("aws.SQS.users") map(new awsUser(_))
  /*  Play.application.configuration.getObjectList("aws.SQS.users")
      .foreach(java.util.List[_ <: com.typesafe.config.ConfigObject]){

    }*/

    /*  user(0).unwrapped().

    }*/
    //val users = List("arn:aws:iam::953196509655:user/IntelAnalytics_User")
    //val u = util.Arrays.asList("arn:aws:iam::953196509655:user/IntelAnalytics_Web")


    //val policyMap =  Map( "Policy" -> createPermissionPolicy)
    //val pm = java.util.Map<String,String>()
    //policyMap.toMap[String,String]
    //mapAsJavaMap(policyMap).asInstanceOf[java.util.Map[java.lang.String,java.lang.String]]
    //val settigns = new SetQueueAttributesRequest(queueUrl.getQueueUrl, mapAsJavaMap(policyMap))

   /* val t = queueUrl.getQueueUrl

    var gqar = new GetQueueAttributesRequest(queueUrl.getQueueUrl)

    //var bleh = new java.util.Collection[String]()

    val jul: java.util.List[String] = ArrayBuffer("All")

    */

    /*var quedata = sqs.getQueueAttributes(gqar)
    val quel = sqs.listQueues*/
    queueUrl.getQueueUrl
  }
}
