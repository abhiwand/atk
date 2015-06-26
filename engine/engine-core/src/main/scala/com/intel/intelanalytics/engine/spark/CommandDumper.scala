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

package com.intel.intelanalytics.engine.spark

import com.intel.event.{ EventContext, EventLogging }
import com.intel.intelanalytics.component.{ Archive, ArchiveDefinition, DefaultArchive, FileUtil }
import FileUtil.writeFile
import com.intel.intelanalytics.engine.plugin.Call
import com.intel.intelanalytics.engine.spark.command.{ CommandExecutor, CommandLoader, CommandPluginRegistry, CommandStorageImpl }
import com.intel.intelanalytics.engine.spark.threading.EngineExecutionContext
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
 * without needing to start up the REST server or too many other resources.
 *
 * This is used for generating the Python docs, it isn't part of the running system.
 */
class CommandDumper(archiveDefinition: ArchiveDefinition, classLoader: ClassLoader, config: Config)
    extends DefaultArchive(archiveDefinition, classLoader, config)
    with DbProfileComponent
    with SlickMetaStoreComponent
    with EventLogging {

  implicit val eventContext = EventContext.enter("CommandDumper")

  override lazy val profile = withMyClassLoader {
    withContext("command dumper connecting to metastore") {
      // Initialize a Profile from settings in the config
      val driver = CommandDumperConfig.metaStoreConnectionDriver
      new Profile(Profile.jdbcProfileForDriver(driver),
        connectionString = CommandDumperConfig.metaStoreConnectionUrl,
        driver,
        username = CommandDumperConfig.metaStoreConnectionUsername,
        password = CommandDumperConfig.metaStoreConnectionPassword,
        poolMaxActive = 100)
    }
  }

  override def start() = {
    metaStore.initializeSchema()
    val impUser: UserPrincipal = new UserPrincipal(new User(1, None, None, new DateTime(), new DateTime()), List("dumper"))
    implicit val call = Call(impUser, EngineExecutionContext.global)
    val commands = new CommandStorageImpl(metaStore.asInstanceOf[SlickMetaStore])
    lazy val engine = new SparkEngine(
      /*sparkContextManager*/ null,
      commandExecutor,
      commands,
      /*frames*/ null,
      /*graphs*/ null,
      /*models*/ null,
      /*users*/ null,
      /*sparkAutoPartitioner*/ null,
      new CommandPluginRegistry(new CommandLoader)) {}
    Await.ready(engine.getCommands(0, 1), 30 seconds) //make sure engine is initialized
    lazy val commandExecutor: CommandExecutor = new CommandExecutor(engine, commands)
    val commandDefs = engine.getCommandDefinitions()
    val commandDump = "{ \"commands\": [" + commandDefs.map(_.toJson).mkString(",\n") + "] }"
    val currentDir = System.getProperty("user.dir")
    val fileName = currentDir + "/target/command_dump.json"
    writeFile(fileName, commandDump)
    println("Command Dump written to " + fileName)
    eventContext.close()
  }

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
