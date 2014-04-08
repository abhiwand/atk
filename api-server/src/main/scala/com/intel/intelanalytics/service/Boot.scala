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
import com.intel.intelanalytics.repository.{DbProfileComponent, SlickMetaStoreComponent}
import com.intel.intelanalytics.service.v1.ApiV1Service

object Boot extends App {

  EventLogger.setImplementation(new SLF4JLogAdapter())
  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("intelanalytics-api")

  trait V1 extends ApiV1Service with SlickMetaStoreComponent with DbProfileComponent {

    override lazy val profile = new Profile(H2Driver, connectionString = "jdbc:h2:mem:iatest;DB_CLOSE_DELAY=-1",
                                            driver = "org.h2.Driver")

    metaStore.create()

  }

  // create and start our service actor
  class Service extends ApiServiceActor
                        with ApiService
                        with V1

  val service = system.actorOf(Props[Service], "dataframe-service")

  implicit val timeout = Timeout(5.seconds)
  // start a new HTTP server on port 8080 with our service actor as the handler
  IO(Http) ? Http.Bind(service, interface = "localhost", port = 8080)
}
