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

package com.intel.intelanalytics.engine.spark

import com.intel.event.{ EventContext, EventLogging }
import com.intel.intelanalytics.component.{ Archive, ArchiveDefinition, DefaultArchive, FileUtil }
import FileUtil.writeFile
import com.intel.intelanalytics.engine.plugin.Call
import com.intel.intelanalytics.engine.spark.command.{ CommandExecutor, CommandLoader, CommandPluginRegistry, SparkCommandStorage }
import com.intel.intelanalytics.engine.spark.queries.{ QueryExecutor, SparkQueryStorage }
import com.intel.intelanalytics.repository.{ DbProfileComponent, Profile, SlickMetaStoreComponent }
import com.intel.intelanalytics.security.UserPrincipal
import com.typesafe.config.Config
import org.joda.time.DateTime
import com.intel.intelanalytics.domain.{ User, DomainJsonProtocol }
import spray.json._
import DomainJsonProtocol.commandDefinitionFormat
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Special archive for dumping the command and query information to a file
 * without needing to start up the api server or too many other resources.
 *
 * This is used for generating the Python docs, it isn't part of the running system.
 */
class CommandDumper(archiveDefinition: ArchiveDefinition, classLoader: ClassLoader, config: Config)
    extends Archive(archiveDefinition, classLoader, config)
    with DbProfileComponent
    with SlickMetaStoreComponent
    with EventLogging {

  implicit val eventContext = EventContext.enter("CommandDumper")

  override lazy val profile = withContext("command dumper connecting to metastore") {
    // Initialize a Profile from settings in the config
    val driver = CommandDumperConfig.metaStoreConnectionDriver
    new Profile(Profile.jdbcProfileForDriver(driver),
      connectionString = CommandDumperConfig.metaStoreConnectionUrl,
      driver,
      username = CommandDumperConfig.metaStoreConnectionUsername,
      password = CommandDumperConfig.metaStoreConnectionPassword)
  }

  override def start() = {
    metaStore.initializeSchema()
    val impUser: UserPrincipal = new UserPrincipal(new User(1, None, None, new DateTime(), new DateTime()), List("dumper"))
    implicit val call = Call(impUser)
    val commands = new SparkCommandStorage(metaStore.asInstanceOf[SlickMetaStore])
    val queries = new SparkQueryStorage(metaStore.asInstanceOf[SlickMetaStore], null)
    lazy val engine = new SparkEngine(
      /*sparkContextManager*/ null,
      commandExecutor,
      commands,
      /*frames*/ null,
      /*graphs*/ null,
      /*models*/ null,
      /*users*/ null,
      queries,
      queryExecutor,
      /*sparkAutoPartitioner*/ null,
      new CommandPluginRegistry(new CommandLoader)) {}
    Await.ready(engine.getCommands(0, 1), 30 seconds) //make sure engine is initialized
    lazy val commandExecutor: CommandExecutor = new CommandExecutor(engine, commands, null)
    lazy val queryExecutor: QueryExecutor = new QueryExecutor(engine, queries, null)
    val commandDefs = engine.getCommandDefinitions()
    val commandDump = "{ \"commands\": [" + commandDefs.map(_.toJson).mkString(",\n") + "] }"
    val currentDir = System.getProperty("user.dir")
    val fileName = currentDir + "/target/command_dump.json"
    writeFile(fileName, commandDump)
    println("Command Dump written to " + fileName)
    eventContext.close()
  }

  /**
   * Not actually used in this implementation.
   */
  override def getAll[T: ClassManifest](descriptor: String): Seq[T] = ???
}

/**
 *  Command Dumper needs to use basic H-2 setup
 */
object CommandDumperConfig extends SparkEngineConfig {
  override val metaStoreConnectionUrl: String = nonEmptyString("intel.analytics.metastore.connection-h2.url")
  override val metaStoreConnectionDriver: String = nonEmptyString("intel.analytics.metastore.connection-h2.driver")
  override val metaStoreConnectionUsername: String = config.getString("intel.analytics.metastore.connection-h2.username")
  override val metaStoreConnectionPassword: String = config.getString("intel.analytics.metastore.connection-h2.password")
}
