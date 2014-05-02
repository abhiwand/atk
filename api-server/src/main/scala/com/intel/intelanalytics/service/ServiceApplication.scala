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

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.slick.driver.H2Driver
import com.intel.event.EventLogger
import com.intel.event.adapter.SLF4JLogAdapter
import com.intel.intelanalytics.component.{Archive}
import com.intel.intelanalytics.repository.{MetaStoreComponent, DbProfileComponent, SlickMetaStoreComponent}
import com.intel.intelanalytics.service.v1.{V1DataFrameService, ApiV1Service}
import com.intel.intelanalytics.engine.EngineComponent
import com.typesafe.config.{Config, ConfigFactory}
import com.intel.intelanalytics.domain.User
import com.intel.intelanalytics.shared.EventLogging

class ServiceApplication extends Archive
    with DbProfileComponent
    with SlickMetaStoreComponent {

  //TODO: choose database profile from config
  override lazy val profile = new Profile(H2Driver, connectionString = "jdbc:h2:mem:iatest;DB_CLOSE_DELAY=-1",
    driver = "org.h2.Driver")

  def get[T](descriptor: String) : T = {
    throw new IllegalArgumentException("This component provides no services")
  }

  def stop() = {}

  def start(configuration: Map[String,String]) = {
    val config = ConfigFactory.load()

    ServiceHost.start(config)

    //TODO: only create if the datatabase doesn't already exist. So far this is in-memory only,
    //but when we want to use postgresql or mysql or something, we won't usually be creating tables here.
    metaStore.create()
    //populate the database with some test users from the specified file (for testing)
    val usersFile = config.getString("test.users.file")
    //read from the resources folder
    val source = scala.io.Source.fromURL(getClass.getResource("/" + usersFile))
    try {
      metaStore.withSession("Populating test users") {
        implicit session =>
          for (line <- source.getLines() if !line.startsWith("#")) {
            val cols: Array[String] = line.split(",")
            val userId:Long = cols(0).trim.toLong
            val apiKey = cols(1).trim
            metaStore.userRepo.insert(new User(userId, apiKey))
          }
      }
    }
    finally{
      source.close()
    }
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
  with EngineComponent {

    //TODO: choose database profile from config
    override lazy val profile = new Profile(H2Driver, connectionString = "jdbc:h2:mem:iatest;DB_CLOSE_DELAY=-1",
      driver = "org.h2.Driver")

    override val engine = com.intel.intelanalytics.component.Boot.getArchive(
      "engine", "com.intel.intelanalytics.engine.EngineApplication").get[Engine]("engine")

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
    Runtime.getRuntime.addShutdownHook(new Thread(){
      override def run(): Unit = {
        com.intel.intelanalytics.component.Boot.getArchive(
          "engine", "com.intel.intelanalytics.engine.EngineApplication").stop()
      }
    })
  }
}