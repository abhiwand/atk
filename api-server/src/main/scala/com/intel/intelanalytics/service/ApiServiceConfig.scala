package com.intel.intelanalytics.service

import com.intel.intelanalytics.shared.SharedConfig
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory

/**
 * Configuration settings for ApiServer.
 *
 * This is our wrapper for Typesafe config.
 */
object ApiServiceConfig extends SharedConfig {

  /** Host or interface that ApiService should listen on */
  val host = config.getString("intel.analytics.api.host")

  /** Port that ApiService should listen on */
  val port = config.getInt("intel.analytics.api.port")
  
  val identifier = config.getString("intel.analytics.api.identifier")
  val defaultTimeout: FiniteDuration = config.getInt("intel.analytics.api.defaultTimeout").seconds
  val defaultCount = config.getInt("intel.analytics.api.defaultCount")
  val testUsersFile = config.getString("intel.analytics.test.users.file")
}
