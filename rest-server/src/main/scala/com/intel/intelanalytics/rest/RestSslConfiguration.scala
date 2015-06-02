//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.rest

import java.security.KeyStore
import javax.net.ssl._

import com.intel.event.EventLogging
import spray.io.ServerSSLEngineProvider

/**
 * X509TrustManager to accept all issuers.
 */
object TrustAllX509TrustManager extends X509TrustManager {
  import java.security.cert.X509Certificate
  def checkClientTrusted(chain: Array[X509Certificate], authType: String) = ()

  def checkServerTrusted(chain: Array[X509Certificate], authType: String) = ()

  def getAcceptedIssuers = Array[X509Certificate]()

}

/**
 * Simple SSL Configuration to create SSL Context and ServerSSLEngineProvider
 * [Optional]
 * To deploy a signed certificate and authenticate the same via python client,
 * please convert the jks file to pem format using steps below. [keytool is available as part of jdk installation]
 * keytool -genkey -alias selfsigned -keyalg RSA -keypass changeit -storepass changeit -keystore keystore.jks -validity 360 -keysize 2048
 * keytool -export -alias selfsigned -storepass changeit -file server.cer -keystore keystore.jks
 * keytool -import -v -trustcacerts -alias selfsigned -file server.cer -keystore cacerts.jks -keypass changeit -storepass changeit
 * keytool -list -keystore keystore.jks
 * keytool -importkeystore -srckeystore keystore.jks -destkeystore keystore.pkcs -srcstoretype JKS -deststoretype PKCS12 -alias selfsigned
 * openssl pkcs12 -in keystore.pkcs -out keystore.pem
 */

//https://github.com/spray/spray/blob/master/examples/spray-can/simple-http-server/src/main/scala/spray/examples/MySslConfiguration.scala
trait RestSslConfiguration extends EventLogging {

  implicit val sslContext: SSLContext = {
    val keyStoreResource = RestServerConfig.keyStoreFile
    val password = RestServerConfig.keyStorePassword
    // In case the certificate file exists, add the cacerts.jks file
    // sys.props += ("javax.net.ssl.trustStore" -> "/tmp/cacerts.jks")
    // sys.props += ("javaee.server.name" -> "localhost") // or server ip
    val keyStore = KeyStore.getInstance("jks")
    keyStore.load(getClass.getResourceAsStream(keyStoreResource), password.toCharArray)
    val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(keyStore, password.toCharArray)
    val context = SSLContext.getInstance("TLS")
    context.init(keyManagerFactory.getKeyManagers, Array(TrustAllX509TrustManager), null)
    context
  }

  implicit def sslEngineProvider: ServerSSLEngineProvider = {
    ServerSSLEngineProvider { engine =>
      engine.getSupportedProtocols.foreach(protocol => debug(s"Supported Protocol: $protocol"))
      engine.getSupportedCipherSuites.foreach(cipher => debug(s"Supported Cipher Suite: $cipher"))
      engine.setEnabledProtocols(Array("TLSv1"))
      engine.setEnabledCipherSuites(Array("TLS_DHE_RSA_WITH_AES_256_CBC_SHA"))
      engine
    }
  }
}
