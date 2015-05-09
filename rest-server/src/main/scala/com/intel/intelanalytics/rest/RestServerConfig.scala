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

  /**
   * Mode of invocation for rest-server : normal or scoring engine mode
   * The ATK rest-server can be run in two different modes:
   * 1) the Normal mode where all the services(excluding scoring-service) for models, frames, queries, graphs are available
   * 2) scoring engine mode where ONLY scoring service is available
   * True means scoring engine mode.
   */
  val scoringEngineMode: Boolean = config.getBoolean("intel.analytics.api.scoring-engine")

}