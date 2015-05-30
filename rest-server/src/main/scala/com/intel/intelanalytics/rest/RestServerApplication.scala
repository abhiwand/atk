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

import com.intel.intelanalytics.engine.plugin.{ Call, Invocation }
import scala.concurrent.duration._
import com.intel.event.EventLogging
import com.intel.intelanalytics.component.{ ArchiveDefinition, Archive }
import com.intel.intelanalytics.engine.Engine
import com.typesafe.config.Config
import scala.concurrent.Await
import scala.reflect.ClassTag
import com.intel.intelanalytics.rest.factory.ServiceFactoryCreator

/**
 * A REST application used by client layer to communicate with the Engine.
 *
 * See the 'api_server.sh' to see how the launcher starts the application.
 */
class RestServerApplication(archiveDefinition: ArchiveDefinition, classLoader: ClassLoader, config: Config)
    extends Archive(archiveDefinition, classLoader, config) with EventLogging {

  EventLogging.raw = true
  info("REST server setting log adapter from configuration")

  EventLogging.raw = configuration.getBoolean("intel.analytics.api.logging.raw")
  info("REST server set log adapter from configuration")

  EventLogging.profiling = configuration.getBoolean("intel.analytics.api.logging.profile")
  info(s"REST server profiling: ${EventLogging.profiling}")

  //Direct subsequent archive messages to the normal log
  Archive.logger = s => info(s)
  Archive.logger("Archive logger installed")

  // TODO: implement or remove get()
  def get[T](descriptor: String): T = {
    throw new IllegalArgumentException("This component provides no services")
  }

  /**
   * Main entry point to start the API Service Application
   */
  override def start() = {
    implicit val call = Call(null)
    val engine = initializeEngine()
    val serviceFactory = ServiceFactoryCreator.createFactory(RestServerConfig.serviceMode, RestServerConfig.schemeIsHttps)
    val scheme = if (RestServerConfig.schemeIsHttps) "https" else "http"
    info(s"Binding service to $scheme://${RestServerConfig.host}:${RestServerConfig.port}")
    // Bind the Spray Actor to an HTTP(s) Port
    val serviceInstance = serviceFactory.createInstance(engine)
    serviceFactory.startInstance(serviceInstance)
  }

  /**
   * Initialize API Server dependencies and perform dependency injection as needed.
   */
  private def initializeEngine()(implicit invocation: Invocation): Engine = {

    //TODO: later engine will be initialized in a separate JVM
    lazy val engine = com.intel.intelanalytics.component.Boot.getArchive("engine-core")
      .get[Engine]("engine")

    //make sure engine is initialized
    Await.ready(engine.getCommands(0, 1), 30 seconds)
    engine
  }

  /**
   * Obtain instances of a given class. The keys are established purely
   * by convention.
   *
   * @param descriptor the string key of the desired class instance.
   * @tparam T the type of the requested instances
   * @return the requested instances, or the empty sequence if no such instances could be produced.
   */
  override def getAll[T: ClassTag](descriptor: String): Seq[T] = {
    throw new Exception("REST server provides no components at this time")
  }
}
