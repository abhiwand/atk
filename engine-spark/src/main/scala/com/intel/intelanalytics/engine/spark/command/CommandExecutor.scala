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

import com.intel.intelanalytics.component.{ArchiveName, Boot}
import com.intel.intelanalytics.domain.command.{Command, CommandTemplate, Execution}
import com.intel.intelanalytics.engine.plugin.{FunctionCommand, CommandPlugin}
import com.intel.intelanalytics.engine.spark.context.SparkContextManager
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.engine.spark.{SparkEngine, SparkEngineConfig}
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.shared.EventLogging
import com.intel.intelanalytics.{ClassLoaderAware, NotFoundException}
import spray.json._

import scala.concurrent._
import scala.util.Try

class CommandExecutor(engine: SparkEngine, commands: SparkCommandStorage, contextManager: SparkContextManager)
      extends EventLogging
        with ClassLoaderAware {

  def getCommands(offset: Int, count: Int)(implicit ec: ExecutionContext): Future[Seq[Command]] = withContext("se.getCommands") {
    future {
      commands.scan(offset, count)
    }
  }

  def getCommand(id: Long)(implicit ec:ExecutionContext): Future[Option[Command]] = withContext("se.getCommand") {
    future {
      commands.lookup(id)
    }
  }

  private var commandPlugins: Map[String, CommandPlugin[_, _]] = SparkEngineConfig.archives.flatMap {
    case (archive, className) => Boot.getArchive(ArchiveName(archive, className))
      .getAll[CommandPlugin[_, _]]("CommandPlugin")
      .map(p => (p.name, p))
  }.toMap

  def registerCommand[A,R](command: CommandPlugin[A,R]): CommandPlugin[A,R] = {
    synchronized {
      commandPlugins += (command.name -> command)
    }
    command
  }

  def registerCommand[A <: Product : JsonFormat, R <: Product : JsonFormat](name: String,
                                                  function: (A, UserPrincipal) => R) : CommandPlugin[A, R] =
    registerCommand(FunctionCommand(name, function))

  private def getCommandDefinition(name: String): Option[CommandPlugin[_, _]] = {
    commandPlugins.get(name)
  }

  private def getResult[R: JsonFormat](execution: Future[Execution], ec:ExecutionContext): Future[R] = {
    implicit val e = ec
    for {
      Execution(start, complete) <- execution
      result <- complete
    } yield result.result.get.convertTo[R]
  }

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * @param command the command to run, including name and arguments
   * @param user the user running the command
   * @return a future that includes
   */
  def execute[A, R](command: CommandPlugin[A,R],
                                          arguments: A,
                                          user: UserPrincipal,
                                          executionContext: ExecutionContext): Execution = {
    implicit val ec = executionContext
    val cmd = commands.create(CommandTemplate(command.name, Some(command.serializeArguments(arguments))))
    withMyClassLoader {
      withContext("ce.execute") {
        withContext(command.name) {
          val cmdFuture = future {
            withCommand(cmd) {
              val invocation: SparkInvocation = SparkInvocation(engine, commandId = cmd.id, arguments = cmd.arguments,
                user = user, executionContext = implicitly[ExecutionContext],
                sparkContextFactory = () => contextManager.context(user).sparkContext)

              val funcResult = command(invocation, arguments)
              command.serializeReturn(funcResult)
            }
            commands.lookup(cmd.id).get
          }
          Execution(cmd, cmdFuture)
        }
      }
    }
  }

  def execute[A, R](name: String,
                                          arguments: A,
                                          user: UserPrincipal,
                                          executionContext: ExecutionContext) : Execution= {
    val function = getCommandDefinition(name)
      .getOrElse(throw new NotFoundException("command definition", name))
      .asInstanceOf[CommandPlugin[A,R]]
    execute(function, arguments, user, executionContext)
  }

  def execute[A, R](command: CommandTemplate,
                    user: UserPrincipal,
                    executionContext: ExecutionContext) : Execution = {
    val function = getCommandDefinition(command.name)
      .getOrElse(throw new NotFoundException("command definition", command.name))
      .asInstanceOf[CommandPlugin[A, R]]
    val convertedArgs = function.parseArguments(command.arguments.get)
    execute(function, convertedArgs, user, executionContext)
  }

  def withCommand[T](command: Command)(block: => JsObject): Unit = {
    commands.complete(command.id, Try {
      block
    })
  }
}
