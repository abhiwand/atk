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

package com.intel.taproot.analytics.engine.command

import com.intel.taproot.event.EventLogging
import com.intel.taproot.analytics.domain.User
import com.intel.taproot.analytics.engine.plugin.{ Invocation, Call }
import com.intel.taproot.analytics.engine._
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.exception.ExceptionUtils
import scala.reflect.io.Directory

/**
 * Executes
 */
class CommandDriver extends AbstractEngineComponent(new CommandLoader) {

  /**
   * Execute Command
   * @param commandId id of command to execute
   */
  def execute(commandId: Long): Unit = {
    commands.lookup(commandId) match {
      case None => info(s"Command $commandId not found")
      case Some(command) => {
        val user: Option[User] = command.createdById match {
          case Some(id) => metaStore.withSession("se.command.lookup") {
            implicit session =>
              metaStore.userRepo.lookup(id)
          }
          case _ => None
        }
        implicit val invocation: Invocation = new Call(user match {
          case Some(u) => userStorage.createUserPrincipalFromUser(u)
          case _ => null
        }, EngineExecutionContext.global)
        commandExecutor.executeCommand(command, commandPluginRegistry)(invocation)
      }
    }
  }
}

/**
 * Static methods for CommandDriver. Includes main method for use by SparkSubmit
 */
object CommandDriver {

  /**
   * Usage string if this was being executed from the command line
   */
  def usage() = println("Usage: java -cp engine.jar com.intel.taproot.analytics.component.CommandDriver <command_id>")

  /**
   * Instantiate an instance of the driver and then executing the requested command.
   * @param commandId
   */
  def executeCommand(commandId: Long): Unit = {
    val driver = new CommandDriver
    driver.execute(commandId)
  }

  /**
   * Entry point of CommandDriver for use by SparkSubmit.
   * @param args command line arguments. Requires command id
   */
  def main(args: Array[String]) = {
    if (args.length < 1) {
      usage()
    }
    else {
      if (EventLogging.raw) {
        val config = ConfigFactory.load()
        EventLogging.raw = if (config.hasPath("intel.taproot.analytics.engine.logging.raw")) config.getBoolean("intel.taproot.analytics.engine.logging.raw") else true
      } // else rest-server already installed an SLF4j adapter

      println(s"Java Class Path is: ${System.getProperty("java.class.path")}")
      println(s"Current PWD is ${Directory.Current.get.toString()}")

      try {
        /* Set to true as for some reason in yarn cluster mode, this doesn't seem to be set on remote driver container */
        sys.props += Tuple2("SPARK_SUBMIT", "true")
        val commandId = args(0).toLong
        executeCommand(commandId)
      }
      catch {
        case t: Throwable => error(s"Error captured in CommandDriver to prevent percolating up to ApplicationMaster + ${ExceptionUtils.getStackTrace(t)}")
      }
      finally {
        sys.props -= "SPARK_SUBMIT"
      }
    }
  }
}
