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
import com.intel.intelanalytics.component.Component
import com.intel.intelanalytics.repository.{DbProfileComponent, SlickMetaStoreComponent}
import com.intel.intelanalytics.service.v1.ApiV1Service
import com.intel.intelanalytics.engine.EngineComponent

class Boot extends Component {


  def get[T](descriptor: String) : T = {
    throw new IllegalArgumentException("This component provides no services")
  }

  def stop() = {}

  def start(configuration: Map[String,String]) = {
    ServiceHost.start()
  }
}


object ServiceHost {
  EventLogger.setImplementation(new SLF4JLogAdapter())

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("intelanalytics-api")

  trait V1 extends ApiV1Service with SlickMetaStoreComponent
  with DbProfileComponent
  with EngineComponent {

    override lazy val profile = new Profile(H2Driver, connectionString = "jdbc:h2:mem:iatest;DB_CLOSE_DELAY=-1",
      driver = "org.h2.Driver")

    metaStore.create()

    override lazy val engine = com.intel.intelanalytics.component.Boot.getComponent(
      "engine", "com.intel.intelanalytics.engine.Boot").get[Engine]("engine")

  }

  // create and start our service actor
  class Service extends ApiServiceActor
  with ApiService
  with V1

  val service = system.actorOf(Props[Service], "api-service")
  implicit val timeout = Timeout(5.seconds)

  def start() = {
    // start a new HTTP server on port 8080 with our service actor as the handler
    IO(Http) ? Http.Bind(service, interface = "localhost", port = 8080)

  }
}