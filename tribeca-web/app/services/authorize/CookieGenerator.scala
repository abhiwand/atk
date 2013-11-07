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

package services.authorize

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Hex
import play.api.mvc.Cookie


/**
 * Generate cookie with the secret and name value
 */
class CookieGenerator {

    private val HMAC_SHA1_ALGORITHM = "HmacSHA1"
    private val MILLISECONDS_PER_SECOND = 1000
    private val UTF8 = "UTF-8"
    private val SECONDS_PER_HOUR = 3600

    def createCookie(secret: String, name: String): Cookie = {
        var checkEmpty = "";
        //temporary fix
        if(secret.isEmpty) checkEmpty = "empty"
        val value = create_signed_value(checkEmpty, name, "localuser")
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

        if(Option(secret) == None)
            throw new IllegalArgumentException

        val timestamp = getEpochTime.toString
        val valueBase64 = new sun.misc.BASE64Encoder().encode(value.getBytes(UTF8))
        val signature = create_signature(secret.getBytes(UTF8), name.getBytes(UTF8), valueBase64.getBytes(UTF8), timestamp.getBytes(UTF8))
        val signatureHex = Hex.encodeHexString(signature)
        val strArray = Array(valueBase64, timestamp, signatureHex)
        strArray.mkString("|")
    }

    def getEpochTime: Long = {
        System.currentTimeMillis / MILLISECONDS_PER_SECOND
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
