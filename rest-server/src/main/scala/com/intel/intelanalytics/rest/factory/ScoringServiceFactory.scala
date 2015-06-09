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

import akka.actor.{ ActorRef, Props }
import akka.io.IO
import com.intel.intelanalytics.engine.Engine
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.rest.{ RestServerConfig, RestSslConfiguration, ScoringService, ScoringServiceActor }
import spray.can.Http
import spray.routing.Directives
import akka.pattern.ask

// Creates a factory for Scoring Service
class ScoringServiceFactory(override val name: String) extends AbstractServiceFactory {
  override def createServiceDefinition(engine: Engine)(implicit invocation: Invocation): ScoringService = {
    new ScoringService(engine)
  }
  override def createActorProps(service: Directives): Props = {
    Props(new ScoringServiceActor(service.asInstanceOf[ScoringService]))
  }
}

class ScoringServiceFactoryOnHttps(override val name: String)
    extends ScoringServiceFactory(name) with RestSslConfiguration {
  override def startInstance(serviceInstance: ActorRef): Unit = {
    IO(Http) ? Http.Bind(serviceInstance, interface = RestServerConfig.host, port = RestServerConfig.port)
  }
}
