//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.service

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import com.intel.event.EventLogger
import com.intel.event.adapter.SLF4JLogAdapter
import com.intel.intelanalytics.component.{ Archive, ArchiveName }
import com.intel.intelanalytics.engine.Engine
import com.typesafe.config.{ Config, ConfigFactory }
import scala.concurrent.Await
import scala.reflect.ClassTag

/**
 * API Service Application - a REST application used by client layer to communicate with the Engine.
 *
 * See the 'api_server.sh' to see how the launcher starts the application.
 */
class ApiServiceApplication extends Archive {

  // TODO: implement or remove get()
  def get[T](descriptor: String): T = {
    throw new IllegalArgumentException("This component provides no services")
  }

  def stop() = {}

  // TODO: delete configuration param?

  /**
   * Main entry point to start the API Service Application
   */
  def start(configuration: Config) = {

    val apiService = initializeDependencies()
    createActorSystemAndBindToHttp(apiService)
  }

  /**
   * Initialize API Server dependencies and perform dependency injection as needed.
   */
  private def initializeDependencies(): ApiService = {

    //TODO: later engine will be initialized in a separate JVM
    lazy val engine = com.intel.intelanalytics.component.Boot.getArchive(
      ArchiveName("engine", "com.intel.intelanalytics.engine.EngineApplication"))
      .get[Engine]("engine")

    //make sure engine is initialized
    Await.ready(engine.getCommands(0, 1), 30 seconds)

    val metaStore = new MetaStoreConfigured().metaStore

    // setup common directives
    val serviceAuthentication = new AuthenticationDirective(metaStore)
    val commonDirectives = new CommonDirectives(serviceAuthentication)

    // setup V1 Services
    val commandService = new v1.CommandService(commonDirectives, engine)
    val dataFrameService = new v1.DataFrameService(commonDirectives, engine)
    val graphService = new v1.GraphService(commonDirectives, engine)
    val apiV1Service = new v1.ApiV1Service(dataFrameService, commandService, graphService)

    // setup main entry point
    new ApiService(commonDirectives, apiV1Service)
  }

  /**
   * We need an ActorSystem to host our application in and to bind it to an HTTP port
   */
  private def createActorSystemAndBindToHttp(apiService: ApiService): Unit = {
    // create the system
    implicit val system = ActorSystem("intelanalytics-api")
    implicit val timeout = Timeout(5.seconds)

    val service = system.actorOf(Props(new ApiServiceActor(apiService)), "api-service")

    // Bind the Spray Actor to an HTTP Port
    // start a new HTTP server with our service actor as the handler
    IO(Http) ? Http.Bind(service, interface = ApiServiceConfig.host, port = ApiServiceConfig.port)
  }

  /**
   * The location at which this component should be installed in the component
   * tree. For example, a graph machine learning algorithm called Loopy Belief
   * Propagation might wish to be installed at
   * "commands/graphs/ml/loopy_belief_propagation". However, it might not actually
   * get installed there if the system has been configured to override that
   * default placement.
   */
  override def defaultLocation: String = "api"

  /**
   * Obtain instances of a given class. The keys are established purely
   * by convention.
   *
   * @param descriptor the string key of the desired class instance.
   * @tparam T the type of the requested instances
   * @return the requested instances, or the empty sequence if no such instances could be produced.
   */
  override def getAll[T: ClassTag](descriptor: String): Seq[T] = {
    throw new Exception("API server provides no components at this time")
  }
}
