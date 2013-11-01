package services.authorize

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Hex
import play.api.mvc.Cookie


/**
 * Created with IntelliJ IDEA.
 * User: schen55
 * Date: 10/30/13
 * Time: 1:39 PM
 * To change this template use File | Settings | File Templates.
 */
object CookieGenerator {

    private val HMAC_SHA1_ALGORITHM = "HmacSHA1";

    def createCookie(secret: String, name: String): Cookie = {
        val value = CookieGenerator.create_signed_value(secret, name, "localuser")
        Cookie(name, value, Some(3600 * 8), "/", Some("intel.com"), true, false)
    }


    def create_signed_value(secret: String, name: String, value: String): String = {
        val timestamp = (System.currentTimeMillis / 1000).toString
        val valueBase64 = new sun.misc.BASE64Encoder().encode(value.getBytes("UTF-8"))
        val signature = create_signature(secret.getBytes("UTF-8"), name.getBytes("UTF-8"), valueBase64.getBytes("UTF-8"), timestamp.getBytes("UTF-8"))
        val signatureHex = Hex.encodeHexString(signature)
        val strArray = Array(valueBase64, timestamp, signatureHex)
        return strArray.mkString("|")
    }

    private def create_signature(secret: Array[Byte], name: Array[Byte], value: Array[Byte], timestamp: Array[Byte]): Array[Byte] =
    {
        val mac: Mac = Mac.getInstance(HMAC_SHA1_ALGORITHM)
        val signingKey = new SecretKeySpec(secret, HMAC_SHA1_ALGORITHM);
        mac.init(signingKey)
        mac.update(name)
        mac.update(value)
        return mac.doFinal(timestamp)
    }

}
