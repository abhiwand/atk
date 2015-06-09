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

package com.intel.intelanalytics.rest.factory

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.pattern.ask
import akka.io.IO
import akka.util.Timeout
import com.intel.event.EventLogging
import com.intel.intelanalytics.engine.Engine
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.rest.RestServerConfig
import spray.can.Http
import spray.routing.Directives

import scala.concurrent.duration._

trait ActorSystemImplicits extends EventLogging {
  // create the system
  implicit val system = ActorSystem("intelanalytics-api")
  implicit val timeout = Timeout(5.seconds)
}

trait AbstractServiceFactory extends ActorSystemImplicits {

  // name of the service
  protected val name: String

  // creates service definition
  protected def createServiceDefinition(engine: Engine)(implicit invocation: Invocation): Directives

  // creates actor given a service definition
  protected def createActorProps(service: Directives): Props

  // starts an instance of the service on http(s) host/port
  def createInstance(engine: Engine)(implicit invocation: Invocation): ActorRef = {
    val serviceDefinition = createServiceDefinition(engine)
    val props = createActorProps(serviceDefinition)
    system.actorOf(props, name)
  }

  def startInstance(serviceInstance: ActorRef): Unit = {
    IO(Http) ? Http.Bind(serviceInstance, interface = RestServerConfig.host, port = RestServerConfig.port)
  }
}
