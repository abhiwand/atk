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

package com.intel.intelanalytics.engine.spark.command

import com.intel.event.EventContext
import com.intel.intelanalytics.engine.plugin.{ CommandInvocation }
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
