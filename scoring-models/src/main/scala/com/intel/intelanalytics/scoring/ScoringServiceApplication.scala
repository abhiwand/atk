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

package com.intel.intelanalytics.scoring

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import com.intel.event.{ EventLogging, EventLogger }
import com.intel.intelanalytics.component.{ ArchiveDefinition, Archive }
import com.typesafe.config.{ Config, ConfigFactory }
import scala.reflect.ClassTag

/**
 * Scoring Service Application - a REST application used by client layer to communicate with the Model.
 *
 * See the 'scoring_server.sh' to see how the launcher starts the application.
 */
class ScoringServiceApplication(archiveDefinition: ArchiveDefinition, classLoader: ClassLoader, config: Config)
    extends Archive(archiveDefinition, classLoader, config) with EventLogging {

  EventLogging.raw = true
  info("Scoring server setting log adapter from configuration")

  EventLogging.raw = configuration.getBoolean("intel.analytics.scoring.logging.raw")
  info("Scoring server set log adapter from configuration")

  EventLogging.profiling = configuration.getBoolean("intel.analytics.scoring.logging.profile")
  info(s"Scoring server profiling: ${EventLogging.profiling}")

  //Direct subsequent archive messages to the normal log
  Archive.logger = s => info(s)
  Archive.logger("Archive logger installed")

  /**
   * Main entry point to start the Scoring Service Application
   */
  override def start() = {
    lazy val modelLoader = com.intel.intelanalytics.component.Boot.getArchive(config.getString("intel.scoring-models.scoring.archive"))
      .load("com.intel.intelanalytics.scoring." + config.getString("intel.scoring-models.scoring.loader"))

    val modelFile = config.getString("intel.scoring-models.scoring.model")

    val service = initializeScoringServiceDependencies(modelLoader.asInstanceOf[ModelLoader], modelFile)

    createActorSystemAndBindToHttp(service)
  }

  private def initializeScoringServiceDependencies(modelLoader: ModelLoader, modelFile: String): ScoringService = {
    val source = scala.io.Source.fromFile(modelFile)
    val byteArray = source.map(_.toByte).toArray
    source.close()

    val model = modelLoader.load(byteArray)
    new ScoringService(model)
  }

  /**
   * We need an ActorSystem to host our application in and to bind it to an HTTP port
   */
  private def createActorSystemAndBindToHttp(scoringService: ScoringService): Unit = {
    // create the system
    implicit val system = ActorSystem("intelanalytics-scoring")
    implicit val timeout = Timeout(5.seconds)

    val service = system.actorOf(Props(new ScoringServiceActor(scoringService)), "scoring-service")

    // Bind the Spray Actor to an HTTP Port
    // start a new HTTP server with our service actor as the handler
    IO(Http) ? Http.Bind(service, interface = config.getString("intel.analytics.scoring.host"), port = config.getInt("intel.analytics.scoring.port"))
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
    throw new Exception("API server provides no components at this time")
  }
}
