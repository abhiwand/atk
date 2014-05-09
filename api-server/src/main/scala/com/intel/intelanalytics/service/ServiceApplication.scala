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

import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.slick.driver.H2Driver
import com.intel.event.EventLogger
import com.intel.event.adapter.SLF4JLogAdapter
import com.intel.intelanalytics.component.{ Archive }
import com.intel.intelanalytics.repository.{ MetaStoreComponent, DbProfileComponent, SlickMetaStoreComponent }
import com.intel.intelanalytics.service.v1.{ V1CommandService, V1DataFrameService, ApiV1Service }
import com.intel.intelanalytics.repository.{ DbProfileComponent, SlickMetaStoreComponent }
import com.intel.intelanalytics.service.v1.{ V1GraphService, V1DataFrameService, ApiV1Service }
import com.intel.intelanalytics.engine.EngineComponent
import com.typesafe.config.{ Config, ConfigFactory }
import com.intel.intelanalytics.domain.{ UserTemplate, User }
import com.intel.intelanalytics.shared.EventLogging
import scala.concurrent.Await

class ServiceApplication extends Archive {

  def get[T](descriptor: String): T = {
    throw new IllegalArgumentException("This component provides no services")
  }

  def stop() = {}

  def start(configuration: Map[String, String]) = {
    val config = ConfigFactory.load()

    ServiceHost.start(config)

  }
}

object ServiceHost {
  EventLogger.setImplementation(new SLF4JLogAdapter())

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("intelanalytics-api")

  trait V1 extends ApiV1Service
      with SlickMetaStoreComponent
      with DbProfileComponent
      with V1DataFrameService
      with V1CommandService
      with V1GraphService
      with EngineComponent {

    ///TODO: choose database profile driver class from config
    override lazy val profile = {
      lazy val config = ConfigFactory.load()

      val connectionString = config.getString("intel.analytics.metastore.connection.url")
      val driver = config.getString("intel.analytics.metastore.connection.driver")
      new Profile(H2Driver, connectionString = connectionString, driver = driver)
    }

    override lazy val engine = com.intel.intelanalytics.component.Boot.getArchive(
      "engine", "com.intel.intelanalytics.engine.EngineApplication").get[Engine]("engine")

    //populate the database with some test users from the specified file (for testing)
    val usersFile = config.getString("intel.analytics.test.users.file")
    //read from the resources folder
    val source = scala.io.Source.fromURL(getClass.getResource("/" + usersFile))
    try {

      //make sure engine is initialized
      Await.ready(engine.getCommands(0, 1), 30 seconds)

      //TODO: Remove when connecting to an actual database server
      metaStore.create()

      metaStore.withSession("Populating test users") {
        implicit session =>
          for (line <- source.getLines() if !line.startsWith("#")) {
            val cols: Array[String] = line.split(",")
            val apiKey = cols(1).trim
            info(s"Creating test user with api key $apiKey")
            metaStore.userRepo.insert(new UserTemplate(apiKey)).get
            assert(metaStore.userRepo.scan().length > 0, "No user was created")
            assert(metaStore.userRepo.retrieveByColumnValue("api_key", apiKey).length == 1, "User not found by api key")
          }
      }
    }
    finally {
      source.close()
    }

  }

  // create and start our service actor
  class Service extends ApiServiceActor
    with ApiService
    with V1

  val service = system.actorOf(Props[Service], "api-service")
  implicit val timeout = Timeout(5.seconds)

  def start(config: Config) = {
    val interface = config.getString("intel.analytics.api.host")
    val port = config.getInt("intel.analytics.api.port")
    // start a new HTTP server with our service actor as the handler
    IO(Http) ? Http.Bind(service, interface = interface, port = port)

    //cleanup stuff on exit
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        com.intel.intelanalytics.component.Boot.getArchive(
          "engine", "com.intel.intelanalytics.engine.EngineApplication").stop()
      }
    })
  }
}