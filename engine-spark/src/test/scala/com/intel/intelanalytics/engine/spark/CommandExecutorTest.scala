package com.intel.intelanalytics.engine.spark

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.intelanalytics.engine.spark.command.{ CommandLoader, SparkCommandStorage, CommandPluginRegistry, CommandExecutor }
import org.specs2.mock.Mockito
import com.intel.intelanalytics.domain.command.{ Command, CommandTemplate }
import scala.concurrent.duration._
import com.intel.intelanalytics.engine.spark.context.SparkContextManager
import org.apache.spark.SparkContext
import scala.concurrent.ExecutionContext.Implicits.global
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.domain.frame.{ CumulativeDist, DataFrame }
import scala.concurrent.{ Await, ExecutionContext }
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol
import DomainJsonProtocol._
import com.intel.intelanalytics.engine.plugin.CommandPlugin
import scala.collection.immutable.HashMap

class CommandExecutorTest extends FlatSpec with Matchers with Mockito {

  val loader = mock[CommandLoader]
  loader.loadFromConfig().returns(new HashMap[String, CommandPlugin[_, _]])

  val commandPluginRegistry = new CommandPluginRegistry(loader)
  def createCommandExecutor(): CommandExecutor = {
    val engine = mock[SparkEngine]
    val commandStorage = mock[SparkCommandStorage]
    val cmd: Command = Command(1, "command", None, None, List(), false, None, null, null, None)
    commandStorage.create(any[CommandTemplate]).returns(cmd)
    commandStorage.lookup(anyLong).returns(Some(cmd))
    val contextManager = mock[SparkContextManager]
    contextManager.context(any[UserPrincipal], anyString).returns(mock[SparkContext])

    new CommandExecutor(engine, commandStorage, contextManager)
  }

  "create spark context" should "add a entry in command id and context mapping" in {
    val args = CumulativeDist[Long](frameId = 1, "", "", "cumulative_sum", "")

    var contextCountDuringExecution = 0
    var containsKey1DuringExecution = false
    val executor = createCommandExecutor()

    val dummyFunc = (dist: CumulativeDist[Long], user: UserPrincipal, invocation: SparkInvocation) => {
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
    val args = CumulativeDist[Long](frameId = 1, "", "", "cumulative_sum", "")
    val executor = createCommandExecutor()

    var contextCountAfterCancel = 0
    val dummyFunc = (dist: CumulativeDist[Long], user: UserPrincipal, invocation: SparkInvocation) => {
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
