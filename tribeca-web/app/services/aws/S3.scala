package services.aws

import sun.misc.BASE64Encoder;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import scala.collection.immutable._
import play.api.libs.json.{JsArray, Json}
import java.util.{TimeZone, Date}
import java.text.SimpleDateFormat
import scala.collection.immutable.Range.BigInt

object S3 {
    val aws_secret_key ="h57vzrHg18IRdGUGnRvfSph381VtuEOfK+r3oNBQ";
    val access_key = "AKIAJ65RQRJONMKNT2NQ";
    val POLICY_EXPIRATION = 604800000 //one week in milliseconds
    val BUCKET = "gaopublic"
    var MAX_SIZE:Long = 5368709120L
    val SUCCESS_ACTION_REDIRECT = "https://localhost/s33"



    def createPolicy(userIdentifier: String): String = {
      val expire = new Date(System.currentTimeMillis() + 604800000)
      //2014-01-01T00:00:00Z
      //val dateFormat = new SimpleDateFormat("EEE, MMM d, yyyy hh:mm:ss a z")
      val dateFormat = new SimpleDateFormat("yyyy-MM-d'T'hh:mm:ss'Z'")
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
      val policyJson = Json.obj("expiration" -> dateFormat.format(expire),
                                "conditions" -> Json.arr( Json.obj("bucket" -> BUCKET),
                                Json.arr("starts-with", "$key", "user/" + userIdentifier + "/" ),
                                Json.obj("acl" -> "private"),
                                //Json.obj("success_action_redirect" -> SUCCESS_ACTION_REDIRECT),
                                //Json.arr("starts-with", "$Content-Type", ""),
                                Json.arr("content-length-range", 0, MAX_SIZE))
                                )
      Json.stringify(policyJson)
    }
    def encodePolicy(policy_document: String): String = {
        (new BASE64Encoder()).encode(policy_document.getBytes("UTF-8")).replaceAll("\n","").replaceAll("\r","");
    }

    def getPolicy(userIdentifier: String): String = {
      val json = createPolicy(userIdentifier)
      encodePolicy(json)
    }

    def getSignature(implicit policy:String): String = {
      createSignature(policy)
    }
    def createSignature(policy: String): String = {
        val hmac = Mac.getInstance("HmacSHA1");
        hmac.init(new SecretKeySpec(aws_secret_key.getBytes("UTF-8"), "HmacSHA1"));
        (new BASE64Encoder()).encode(hmac.doFinal(policy.getBytes("UTF-8"))).replaceAll("\n", "");
    }
}
