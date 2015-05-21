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

package com.intel.intelanalytics.rest.factory

import akka.actor.{ ActorRef, Props }
import akka.io.IO
import com.intel.intelanalytics.engine.Engine
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.rest._
import spray.can.Http
import spray.routing.Directives
import akka.pattern.ask

// Creates a factory for Api Service with support for all routes
class StandardServiceFactory(override val name: String) extends AbstractServiceFactory {
  override def createServiceDefinition(engine: Engine)(implicit invocation: Invocation): ApiService = {
    // setup common directives
    val serviceAuthentication = new AuthenticationDirective(engine)
    val commonDirectives = new CommonDirectives(serviceAuthentication)

    // setup V1 Services
    val commandService = new v1.CommandService(commonDirectives, engine)
    val dataFrameService = new v1.FrameService(commonDirectives, engine)
    val graphService = new v1.GraphService(commonDirectives, engine)
    val modelService = new v1.ModelService(commonDirectives, engine)
    val queryService = new v1.QueryService(commonDirectives, engine)
    val apiV1Service = new v1.ApiV1Service(dataFrameService, commandService, graphService, modelService, queryService)

    // setup main entry point
    new ApiService(commonDirectives, apiV1Service)
  }
  override def createActorProps(service: Directives): Props = {
    Props(new ApiServiceActor(service.asInstanceOf[ApiService]))
  }
}

class StandardServiceFactoryOnHttps(override val name: String)
  extends StandardServiceFactory(name) with RestSslConfiguration {
  override def startInstance(serviceInstance: ActorRef): Unit = {
    IO(Http) ? Http.Bind(serviceInstance, interface = RestServerConfig.host, port = RestServerConfig.port)
  }
}