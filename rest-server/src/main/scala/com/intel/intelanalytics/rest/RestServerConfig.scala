/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.rest

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory

/**
 * Configuration settings for ApiServer.
 *
 * This is our wrapper for Typesafe config.
 */
object RestServerConfig {

  val config = ConfigFactory.load()

  // val's are not lazy because failing early is better

  /** Host or interface that ApiService should listen on */
  val host: String = config.getString("intel.analytics.api.host")

  /** Port that ApiService should listen on */
  val port: Int = config.getInt("intel.analytics.api.port")

  /** A String describing the service, e.g. "ia" */
  val identifier: String = config.getString("intel.analytics.api.identifier")

  /** Default timeout for actors */
  val defaultTimeout: FiniteDuration = config.getDuration("intel.analytics.api.default-timeout", TimeUnit.SECONDS).seconds

  /** Default number of items to return in service index when not specified. E.g. GET /v1/commands */
  val defaultCount: Int = config.getInt("intel.analytics.api.default-count")

  val buildId = config.getString("intel.analytics.api.buildId")

  /** sentinel token which avoids oauth */
  val shortCircuitApiKey = config.getString("intel.analytics.component.archives.rest-server.shortCircuitApiKey")

  /** the space id of this ATK instance */ // todo - get from CC or VCAPS
  val appSpace = config.getString("intel.analytics.component.archives.rest-server.appSpace")

  /** the URI of the Cloud Controller */
  val ccUri = config.getString("intel.analytics.component.archives.rest-server.ccUri")

  /** the URI of the UAA server */
  val uaaUri = config.getString("intel.analytics.component.archives.rest-server.uaaUri")

  /** How many seconds to cache user principals, helpful for high request volume (e.g. QA parallel testing) */
  val userPrincipalCacheTimeoutSeconds = config.getInt("intel.analytics.component.archives.rest-server.user-principal-cache.timeout-seconds")

  /** Max size of user principals cache */
  val userPrincipalCacheMaxSize = config.getInt("intel.analytics.component.archives.rest-server.user-principal-cache.max-size")

  /** Max number of threads per execution context */
  val maxThreadsPerExecutionContext: Int = config.getInt("intel.analytics.max-threads-per-execution-Context")

  /**
   * Mode of invocation for api-server : standard or scoring mode
   * The ATK Server can be run in two different modes:
   * 1) standard mode where all the services(excluding scoring-service) for models, frames, queries, graphs are available
   * 2) scoring mode where ONLY scoring service is available
   */
  val serviceMode: String = config.getString("intel.analytics.api.service-mode")

  /** Scheme for Rest Service to bind with (http or https) */
  val schemeIsHttps: Boolean = config.getBoolean("spray.can.server.ssl-encryption")

  /** Location of the Java keystore file */
  val keyStoreFile: String = config.getString("intel.analytics.component.archives.rest-server.key-store-file")

  /** Password for the keystore file */
  val keyStorePassword: String = config.getString("intel.analytics.component.archives.rest-server.key-store-password")
}
