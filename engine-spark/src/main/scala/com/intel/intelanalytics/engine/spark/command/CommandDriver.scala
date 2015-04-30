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

package com.intel.intelanalytics.engine.spark.command
import com.intel.event.{ EventLogging }
import com.intel.intelanalytics.domain.User
import com.intel.intelanalytics.engine.plugin.{ Invocation, Call }
import com.intel.intelanalytics.engine.spark._
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
        })
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
  def usage() = println("Usage: java -cp engine-spark.jar com.intel.intelanalytics.component.CommandDriver <command_id>")

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
        EventLogging.raw = if (config.hasPath("intel.analytics.engine.logging.raw")) config.getBoolean("intel.analytics.engine.logging.raw") else true
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
