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

package com.intel.intelanalytics.engine.spark.command

import com.intel.event.EventContext
import com.intel.intelanalytics.engine.plugin.{ CommandInvocation, Invocation }
import com.intel.intelanalytics.engine.{ CommandStorageProgressUpdater, ReferenceResolver, CommandStorage, Engine }
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.JsObject

import scala.concurrent.ExecutionContext
import com.intel.intelanalytics.engine.spark.CommandProgressUpdater

/**
 * Basic invocation for commands that don't need Spark
 */
case class SimpleInvocation(engine: Engine,
                            commandStorage: CommandStorage,
                            executionContext: ExecutionContext,
                            arguments: Option[JsObject],
                            commandId: Long,
                            user: UserPrincipal,
                            resolver: ReferenceResolver,
                            eventContext: EventContext) extends CommandInvocation {
  override val progressUpdater: CommandProgressUpdater = new CommandStorageProgressUpdater(commandStorage)
}