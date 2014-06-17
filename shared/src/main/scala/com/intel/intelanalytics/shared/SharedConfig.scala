package com.intel.intelanalytics.shared

import com.typesafe.config.ConfigFactory

/**
 * Configuration that is shared between both the API Server and the Engine.
 *
 * This is our wrapper for Typesafe config.
 *
 * See ApiServiceConfig and SparkEngineConfig.
 */
trait SharedConfig {

  val config = ConfigFactory.load().withFallback(ConfigFactory.load("engine.conf"))

  // val's are not lazy because failing early is better
  val metaStoreConnectionUrl: String = nonEmptyString("intel.analytics.metastore.connection.url")
  val metaStoreConnectionDriver: String = nonEmptyString("intel.analytics.metastore.connection.driver")
  val metaStoreConnectionUsername: String = config.getString("intel.analytics.metastore.connection.username")
  val metaStoreConnectionPassword: String = config.getString("intel.analytics.metastore.connection.password")

  /**
   * Get a String but throw Exception if it is empty
   */
  protected def nonEmptyString(key: String): String = {
    config.getString(key) match {
      case "" => throw new IllegalArgumentException(key + " cannot be empty!")
      case s: String => s
    }
  }

}
