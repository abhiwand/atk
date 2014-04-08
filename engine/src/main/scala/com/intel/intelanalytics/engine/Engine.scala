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

package com.intel.intelanalytics.engine

import akka.actor.Actor
import com.intel.intelanalytics.shared.EventLogging

import scala.concurrent.ExecutionContext.Implicits.global

/** This is the Akka interface to the engine */
class ApiServiceActor extends Actor with EventLogging { this: EngineComponent =>

  def receive = {
    case AppendFile(id, fileName, rowParser) => for {
      f <- engine.getFrame(id)
      res <- engine.appendFile(f, fileName, rowParser)
    } yield res
//    case AddColumn(id, name, map) => engine.addColumn(id, name, map)
//    case DropColumn(id, name) => engine.dropColumn(id, name)
//    case DropRows(id, filter) => engine.dropRows(id, filter)
    case x => warn("Unknown message: " + x)
  }

}

case class AppendFile(id: Long, fileName: String, rowGenerator: Functional)
case class DropColumn(id: Long, name: String)
case class AddColumn(id: Long, name: String, map: Option[RowFunction[Any]])
case class DropRows(id: Long, filter: RowFunction[Boolean])

