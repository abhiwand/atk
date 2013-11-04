package services.authorize

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Hex
import play.api.mvc.Cookie


/**
 * Generate cookie with the secret and name value
 */
object CookieGenerator {

    private val HMAC_SHA1_ALGORITHM = "HmacSHA1"
    private val MILLISECONDS_PER_SECOND = 1000
    private val UTF8 = "UTF-8"
    private val SECONDS_PER_HOUR = 3600

    def createCookie(secret: String, name: String): Cookie = {
        val value = CookieGenerator.create_signed_value(secret, name, "localuser")
        Cookie(name, value, Some(SECONDS_PER_HOUR * 8), "/", Some("intel.com"), false, false)
    }

    /**
     *
     * Use the secret to generate cookie content with name and value.
     * @param secret
     * @param name
     * @param value
     * @return
     */
    def create_signed_value(secret: String, name: String, value: String): String = {
        val timestamp = (System.currentTimeMillis / MILLISECONDS_PER_SECOND).toString
        val valueBase64 = new sun.misc.BASE64Encoder().encode(value.getBytes(UTF8))
        val signature = create_signature(secret.getBytes(UTF8), name.getBytes(UTF8), valueBase64.getBytes(UTF8), timestamp.getBytes(UTF8))
        val signatureHex = Hex.encodeHexString(signature)
        val strArray = Array(valueBase64, timestamp, signatureHex)
        strArray.mkString("|")
    }

    /**
     *
     * Create signature with the secret.
     * @param secret
     * @param name
     * @param value
     * @param timestamp
     * @return
     */
    private def create_signature(secret: Array[Byte], name: Array[Byte], value: Array[Byte], timestamp: Array[Byte]): Array[Byte] =
    {
        val mac: Mac = Mac.getInstance(HMAC_SHA1_ALGORITHM)
        val signingKey = new SecretKeySpec(secret, HMAC_SHA1_ALGORITHM)
        mac.init(signingKey)
        mac.update(name)
        mac.update(value)
        mac.doFinal(timestamp)
    }

}
