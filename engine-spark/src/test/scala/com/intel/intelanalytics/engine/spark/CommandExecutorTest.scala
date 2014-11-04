package com.intel.intelanalytics.engine.spark

import com.intel.intelanalytics.engine.{ProgressInfo, CommandStorage}
import org.joda.time.DateTime
import org.scalatest.{ Matchers, FlatSpec }
import com.intel.intelanalytics.engine.spark.command.{ CommandLoader, SparkCommandStorage, CommandPluginRegistry, CommandExecutor }
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.intel.intelanalytics.domain.command.{ Command, CommandTemplate }
import scala.concurrent.duration._
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import org.apache.spark.SparkContext
import scala.concurrent.ExecutionContext.Implicits.global
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.domain.frame.{ CumulativeSum, DataFrame }
import com.intel.intelanalytics.domain.frame.QuantileValues
import scala.concurrent.{ Await, ExecutionContext }
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol
import DomainJsonProtocol._
import com.intel.intelanalytics.engine.plugin.CommandPlugin
import scala.collection.immutable.HashMap
import org.scalatest.mock.MockitoSugar

import scala.util.Try
import scala.collection._

class FakeCommandStorage extends CommandStorage {
  var commands: Map[Long, Command] = Map.empty
  var counter = 1L
  
  override def lookup(id: Long): Option[Command] = commands.get(id)

  override def scan(offset: Int, count: Int): Seq[Command] = commands.values.toSeq

  override def complete(id: Long, result: Try[JsObject]): Unit = {}

  /**
   * update command info regarding progress of jobs initiated by this command
   * @param id command id
   * @param progressInfo list of progress for the jobs initiated by this command
   */
  override def updateProgress(id: Long, progressInfo: List[ProgressInfo]): Unit = {}  

  override def create(template: CommandTemplate): Command = {
    val id = counter
    counter+=1
    val command: Command = Command(id, template.name, template.arguments, createdOn = DateTime.now, modifiedOn = DateTime.now)
    commands += (id -> command)
    command
  }

  override def start(id: Long): Unit = ???
}
class CommandExecutorTest extends FlatSpec with Matchers with MockitoSugar {

  val loader = mock[CommandLoader]
  when(loader.loadFromConfig()).thenReturn(new HashMap[String, CommandPlugin[_, _]])

  val commandPluginRegistry = new CommandPluginRegistry(loader)
  def createCommandExecutor(): CommandExecutor = {
    val engine = mock[SparkEngine]
    val commandStorage = new FakeCommandStorage
    val contextFactory = mock[SparkContextFactory]
    val sc = mock[SparkContext]
    when(contextFactory.context(any(classOf[UserPrincipal]), anyString(), Some(anyString()))).thenReturn(sc)

    new CommandExecutor(engine, commandStorage, contextFactory)
  }

  "create spark context" should "add a entry in command id and context mapping" in {
    val args = QuantileValues(List())
    var contextCountDuringExecution = 0
    var containsKey1DuringExecution = false
    val executor = createCommandExecutor()

    val dummyFunc = (dist: QuantileValues, user: UserPrincipal, invocation: SparkInvocation) => {
      contextCountDuringExecution = executor.commandIdContextMapping.size
      containsKey1DuringExecution = executor.commandIdContextMapping.contains(1)
      mock[DataFrame]
    }

    commandPluginRegistry.registerCommand("dummy", dummyFunc)
    val user = mock[UserPrincipal]
    val execution = executor.execute(CommandTemplate(name = "dummy", arguments = Some(args.toJson.asJsObject())),
      user, implicitly[ExecutionContext], commandPluginRegistry)
    Await.ready(execution.end, 10 seconds)
    contextCountDuringExecution shouldBe 1
    containsKey1DuringExecution shouldBe true

    //make sure the entry is cleaned up after execution
    executor.commandIdContextMapping.size shouldBe 0
  }

  "cancel command during execution" should "remove the entry from command id and context mapping" in {
    val args = QuantileValues(List())
    val executor = createCommandExecutor()

    var contextCountAfterCancel = 0
    val dummyFunc = (dist: QuantileValues, user: UserPrincipal, invocation: SparkInvocation) => {
      executor.stopCommand(1)
      contextCountAfterCancel = executor.commandIdContextMapping.size
      mock[DataFrame]
    }

    commandPluginRegistry.registerCommand("dummy", dummyFunc)
    val user = mock[UserPrincipal]
    val execution = executor.execute(CommandTemplate(name = "dummy", arguments = Some(args.toJson.asJsObject())),
                                      user, implicitly[ExecutionContext], commandPluginRegistry)
    Await.ready(execution.end, 10 seconds)
    contextCountAfterCancel shouldBe 0
  }

}
