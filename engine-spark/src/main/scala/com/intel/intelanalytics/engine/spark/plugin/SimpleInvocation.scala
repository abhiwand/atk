package com.intel.intelanalytics.engine.spark.plugin

/**
 * Created by joyeshmi on 7/2/14.
 */
import com.intel.intelanalytics.engine.Engine
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.security.UserPrincipal
import spray.json.JsObject

import scala.concurrent.ExecutionContext

/**
 * Captures details of a particular invocation of a command. This instance is passed to the
 * command's execute method along with the (converted) arguments supplied by the caller.
 *
 * @param engine an instance of the Engine for use by the command
 * @param user the calling user
 * @param commandId the ID assigned to this command execution
 * @param executionContext the Scala execution context in use
 * @param arguments the original JSON arguments, unconverted
 */
case class SimpleInvocation(engine: Engine,
                            user: UserPrincipal,
                            commandId: Long,
                            executionContext: ExecutionContext,
                            arguments: Option[JsObject]) extends Invocation