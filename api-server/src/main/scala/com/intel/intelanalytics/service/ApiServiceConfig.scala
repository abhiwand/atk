package com.intel.intelanalytics.service

import com.intel.intelanalytics.shared.SharedConfig

import scala.concurrent.duration._

/**
 * Configuration settings for ApiServer.
 *
 * This is our wrapper for Typesafe config.
 */
object ApiServiceConfig extends SharedConfig {

  /** Host or interface that ApiService should listen on */
  val host: String = config.getString("intel.analytics.api.host")

  /** Port that ApiService should listen on */
  val port: Int = config.getInt("intel.analytics.api.port")

  /** A String describing the service, e.g. "ia" */
  val identifier: String = config.getString("intel.analytics.api.identifier")

  /** Default timeout for actors */
  val defaultTimeout: FiniteDuration = config.getInt("intel.analytics.api.defaultTimeout").seconds

  /** Default number of items to return in service index when not specified. E.g. GET /v1/commands */
  val defaultCount: Int = config.getInt("intel.analytics.api.defaultCount")

  /** Input file for creating test users for local development */
  val testUsersFile: String = config.getString("intel.analytics.test.users.file")
}
