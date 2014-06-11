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

import com.intel.intelanalytics.security.UserPrincipal
import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.engine.spark.ClassLoaderAware
import com.typesafe.config.Config
import spray.json.JsObject
import com.intel.intelanalytics.shared.EventLogging

object Execution {

  case class CommandExecution(engine: Engine,
                              config: Config,
                              commandId: Long,
                              arguments: Option[JsObject],
                              user: UserPrincipal,
                              implicit val executionContext: ExecutionContext)

  /**
   * Commands are the equivalent of functions - units of computation. They
   * are invoked by the engine, and additional commands can be added as
   * plugins.
   */
  trait CommandDefinition extends (CommandExecution => JsObject)
  with EventLogging
  with ClassLoaderAware {

    protected def execute(execution: CommandExecution) : JsObject

    /**
     * Overridden to ensure that the execute method is called with the command's
     * class loader as the context class loader.
     * @param execution the invocation information
     * @return the result of the command
     */
    final def apply(execution: CommandExecution) = {
      withMyClassLoader {
        execute(execution)
      }
    }
  }
}