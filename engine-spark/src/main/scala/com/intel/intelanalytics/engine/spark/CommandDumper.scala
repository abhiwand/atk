package com.intel.intelanalytics.engine.spark

import com.intel.intelanalytics.component.{ Boot, DefaultArchive }
import com.intel.intelanalytics.repository.{ Profile, DbProfileComponent, SlickMetaStoreComponent }
import com.intel.intelanalytics.shared.{ SharedConfig, EventLogging }
import com.intel.intelanalytics.engine.spark.queries.{ QueryExecutor, SparkQueryStorage }
import com.intel.intelanalytics.engine.spark.command.{ CommandLoader, CommandPluginRegistry, CommandExecutor, SparkCommandStorage }
import com.intel.intelanalytics.domain.{ User, DomainJsonProtocol }
import scala.concurrent.Await
import scala.concurrent.duration._
import spray.json._
import DomainJsonProtocol.commandDefinitionFormat
import com.intel.intelanalytics.security.UserPrincipal
import org.joda.time.DateTime

/**
 * Special archive for dumping the command and query information to a file
 * without needing to start up the api server or too many other resources.
 */
class CommandDumper extends DefaultArchive
    with DbProfileComponent
    with SlickMetaStoreComponent
    with EventLogging {

  override lazy val profile = withContext("command dumper connecting to metastore") {
    Profile.initializeFromConfig(CommandDumperConfig)
  }

  override def start() = {
    metaStore.initializeSchema()
    val commands = new SparkCommandStorage(metaStore.asInstanceOf[SlickMetaStore])
    val queries = new SparkQueryStorage(metaStore.asInstanceOf[SlickMetaStore], null)
    lazy val engine = new SparkEngine(
      /*sparkContextManager*/ null,
      commandExecutor,
      commands,
      /*frames*/ null,
      /*graphs*/ null,
      queries,
      queryExecutor,
      /*sparkAutoPartitioner*/ null,
      new CommandPluginRegistry(new CommandLoader)) {}
    Await.ready(engine.getCommands(0, 1), 30 seconds) //make sure engine is initialized
    lazy val commandExecutor: CommandExecutor = new CommandExecutor(engine, commands, null)
    lazy val queryExecutor: QueryExecutor = new QueryExecutor(engine, queries, null)
    implicit val impUser: UserPrincipal = new UserPrincipal(new User(1, None, None, new DateTime(), new DateTime()), List("dumper"))
    val commandDefs = engine.getCommandDefinitions()
    val commandDump = "{ \"commands\": [" + commandDefs.map(_.toJson).mkString(",\n") + "] }"
    val currentDir = System.getProperty("user.dir")
    val fileName = currentDir + "/target/command_dump.json"
    Boot.writeFile(fileName, commandDump)
    println("Command Dump written to " + fileName)
  }
}

/**
 *  Command Dumper needs to use basic H-2 setup
 */
object CommandDumperConfig extends SharedConfig {
  override val metaStoreConnectionUrl: String = nonEmptyString("intel.analytics.metastore.connection-h2.url")
  override val metaStoreConnectionDriver: String = nonEmptyString("intel.analytics.metastore.connection-h2.driver")
  override val metaStoreConnectionUsername: String = config.getString("intel.analytics.metastore.connection-h2.username")
  override val metaStoreConnectionPassword: String = config.getString("intel.analytics.metastore.connection-h2.password")
}