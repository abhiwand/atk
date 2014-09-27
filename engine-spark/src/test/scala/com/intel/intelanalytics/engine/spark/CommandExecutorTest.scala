package com.intel.intelanalytics.engine.spark

import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.command.{ Command, CommandTemplate }
import com.intel.intelanalytics.domain.frame.{FrameReference, DataFrame, QuantileValues}
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{Action, CommandPlugin, Invocation}
import com.intel.intelanalytics.engine.spark.command.{ CommandExecutor, CommandLoader, CommandPluginRegistry, SparkCommandStorage }
import com.intel.intelanalytics.engine.spark.context.SparkContextManager
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.SparkContext
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FlatSpec, Matchers }
import spray.json.JsObject

import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext }

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

  "running a command" should "add an entry in command id and context mapping for SparkCommands" in {
    val args = QuantileValues(List())
    var contextCountDuringExecution = 0
    var containsKey1DuringExecution = false
    val executor = createCommandExecutor()

    val dummyFunc = (dist: QuantileValues, user: UserPrincipal, invocation: SparkInvocation) => {
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

  it should "not add an entry in command id and context mapping for regular commands" in {
    val args = QuantileValues(List())
    var contextCountDuringExecution = 0
    var containsKey1DuringExecution = false
    val executor = createCommandExecutor()

    val plugin = new CommandPlugin[QuantileValues, DataFrame] {

      implicit val qformat = jsonFormat1(QuantileValues)

      implicit val dformat = DomainJsonProtocol.dataFrameFormat

      def parseArguments(arguments: JsObject): QuantileValues = qformat.read(arguments)

      def serializeArguments(arguments: QuantileValues): JsObject = qformat.write(arguments).asJsObject

      override def serializeReturn(returnValue: DataFrame): JsObject = dformat.write(returnValue).asJsObject

      override def name: String = "foo"

      def execute(invocation: Invocation, arguments: QuantileValues)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame = {

        contextCountDuringExecution = executor.commandIdContextMapping.size
        containsKey1DuringExecution = executor.commandIdContextMapping.contains(1)
        mock[DataFrame]
      }
    }

    val user = mock[UserPrincipal]
    val execution = executor.execute(plugin, args, user, implicitly[ExecutionContext])
    Await.ready(execution.end, 10 seconds)
    contextCountDuringExecution shouldBe 0
    containsKey1DuringExecution shouldBe false

    //make sure the mapping is still empty
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

    val plugin = commandPluginRegistry.registerCommand("dummy", dummyFunc)
    val user = mock[UserPrincipal]
    val execution = executor.execute(plugin, args, user, implicitly[ExecutionContext])
    Await.ready(execution.end, 10 seconds)
    contextCountAfterCancel shouldBe 0
  }

  "getReferenceTypes" should "find frame references and graph references" in {
    case class Foo(frameId: Int, frame: FrameReference, graphId: Int, graph: GraphReference)

    val executor = createCommandExecutor()

    val members = executor.getReferenceTypes[Foo]().toArray

    members.length shouldBe 2
    members.map(_._1).toArray shouldBe Array("frame", "graph")
  }

  "isAction" should "recognize Actions as actions" in {

    //Foo has only UriReference types, so returning it doesn't make this automatically an action
    case class Foo(frame: FrameReference)

    val executor = createCommandExecutor()

    //Declare a plugin "with Action"
    val plugin = new CommandPlugin[Foo,Foo] with Action {
      override def parseArguments(arguments: JsObject): Foo = ???
      override def serializeArguments(arguments: Foo): JsObject = ???
      override def serializeReturn(returnValue: Foo): JsObject = ???
      override def execute(invocation: Invocation, arguments: Foo)(implicit user: UserPrincipal, executionContext: ExecutionContext): Foo = ???
      override def name: String = ???
    }

    executor.isAction(plugin) shouldBe true

  }

  it should "recognize things that return types with non-UriReference members as actions" in {

    //Foo has both UriReference and non-UriReference types, so returning it makes this plugin an action.
    case class Foo(frameId: Int, frame: FrameReference)

    val executor = createCommandExecutor()

    //Declare a plugin without Action
    val plugin = new CommandPlugin[Foo,Foo] {
      override def parseArguments(arguments: JsObject): Foo = ???
      override def serializeArguments(arguments: Foo): JsObject = ???
      override def serializeReturn(returnValue: Foo): JsObject = ???
      override def execute(invocation: Invocation, arguments: Foo)(implicit user: UserPrincipal, executionContext: ExecutionContext): Foo = ???
      override def name: String = ???
    }

    executor.isAction(plugin) shouldBe true

  }

  it should "return false for a non-Action that returns only UriReference properties" in {

    //Foo has only UriReference types, so returning it doesn't automatically make this an action.
    case class Foo(frame: FrameReference)

    val executor = createCommandExecutor()

    //Declare a plugin without Action
    val plugin = new CommandPlugin[Foo,Foo] {
      override def parseArguments(arguments: JsObject): Foo = ???
      override def serializeArguments(arguments: Foo): JsObject = ???
      override def serializeReturn(returnValue: Foo): JsObject = ???
      override def execute(invocation: Invocation, arguments: Foo)(implicit user: UserPrincipal, executionContext: ExecutionContext): Foo = ???
      override def name: String = ???
    }

    executor.isAction(plugin) shouldBe false

  }
}
