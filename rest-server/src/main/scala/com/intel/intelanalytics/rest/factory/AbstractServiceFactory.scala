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