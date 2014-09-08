package com.intel.intelanalytics.engine.spark

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.intelanalytics.engine.spark.command.{ CommandLoader, SparkCommandStorage, CommandPluginRegistry, CommandExecutor }
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.intel.intelanalytics.domain.command.{ Command, CommandTemplate }
import scala.concurrent.duration._
import com.intel.intelanalytics.engine.spark.context.SparkContextManager
import org.apache.spark.SparkContext
import scala.concurrent.ExecutionContext.Implicits.global
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.domain.frame.{ CumulativeSum, DataFrame }
import com.intel.intelanalytics.domain.frame.PercentileValues
import scala.concurrent.{ Await, ExecutionContext }
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol
import DomainJsonProtocol._
import com.intel.intelanalytics.engine.plugin.CommandPlugin
import scala.collection.immutable.HashMap
import org.scalatest.mock.MockitoSugar

class CommandExecutorTest extends FlatSpec with Matchers with MockitoSugar {

  val loader = mock[CommandLoader]
  when(loader.loadFromConfig()).thenReturn(new HashMap[String, CommandPlugin[_, _]])

  val commandPluginRegistry = new CommandPluginRegistry(loader)
  def createCommandExecutor(): CommandExecutor = {
    val engine = mock[SparkEngine]
    val commandStorage = mock[SparkCommandStorage]
    val cmd: Command = Command(1, "command", None, None, List(), false, None, null, null, None)
    when(commandStorage.create(any(classOf[CommandTemplate]))).thenReturn(cmd)
    when(commandStorage.lookup(anyLong())).thenReturn(Some(cmd))
    val contextManager = mock[SparkContextManager]
    val sc = mock[SparkContext]
    when(contextManager.context(any(classOf[UserPrincipal]), anyString())).thenReturn(sc)

    new CommandExecutor(engine, commandStorage, contextManager)
  }

  "create spark context" should "add a entry in command id and context mapping" in {

    val args = PercentileValues(List())
    var contextCountDuringExecution = 0
    var containsKey1DuringExecution = false
    val executor = createCommandExecutor()

    val dummyFunc = (dist: PercentileValues, user: UserPrincipal, invocation: SparkInvocation) => {
      contextCountDuringExecution = executor.commandIdContextMapping.size
      containsKey1DuringExecution = executor.commandIdContextMapping.contains(1)
      mock[DataFrame]
    }

    val plugin = commandPluginRegistry.registerCommand("dummy", dummyFunc)
    val user = mock[UserPrincipal]
    val execution = executor.execute(plugin, args, user, implicitly[ExecutionContext])
    Await.ready(execution.end, 10 seconds)
    contextCountDuringExecution shouldBe 1
    containsKey1DuringExecution shouldBe true

    //make sure the entry is cleaned up after execution
    executor.commandIdContextMapping.size shouldBe 0
  }

  "cancel command during execution" should "remove the entry from command id and context mapping" in {
    //val args = CumulativeSum(mock[DataFrame], "")
    val args = PercentileValues(List())
    val executor = createCommandExecutor()

    var contextCountAfterCancel = 0
    val dummyFunc = (dist: PercentileValues, user: UserPrincipal, invocation: SparkInvocation) => {
      executor.stopCommand(1)
      contextCountAfterCancel = executor.commandIdContextMapping.size
      mock[DataFrame]
    }

    val plugin = commandPluginRegistry.registerCommand("dummy", dummyFunc)
    val user = mock[UserPrincipal]
    val execution = executor.execute(plugin, args, user, implicitly[ExecutionContext])
    Await.ready(execution.end, 10 seconds)
    contextCountAfterCancel shouldBe 0
  }

}
