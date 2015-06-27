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

import com.intel.intelanalytics.engine.{ ProgressInfo, CommandStorage }
import org.joda.time.DateTime
import org.scalatest.{ Matchers, FlatSpec }
import com.intel.intelanalytics.engine.spark.command._
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.intel.intelanalytics.domain.command.{ Command, CommandTemplate }
import org.apache.spark.SparkContext
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol
import DomainJsonProtocol._
import com.intel.intelanalytics.engine.plugin.{ Invocation, CommandPlugin }
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
  override def storeResult(id: Long, result: Try[JsObject]): Unit = {}

  /**
   * update command info regarding progress of jobs initiated by this command
   * @param id command id
   * @param progressInfo list of progress for the jobs initiated by this command
   */
  override def updateProgress(id: Long, progressInfo: List[ProgressInfo]): Unit = {}

  override def create(template: CommandTemplate): Command = {
    val id = counter
    counter += 1
    val command: Command = Command(id, template.name, template.arguments, createdOn = DateTime.now, modifiedOn = DateTime.now)
    commands += (id -> command)
    command
  }

  override def start(id: Long): Unit = ???
}
class CommandExecutorTest extends FlatSpec with Matchers with MockitoSugar {

  val loader = mock[CommandLoader]
  val commandRegistryMaps = CommandPluginRegistryMaps(new HashMap[String, CommandPlugin[_, _]], new HashMap[String, String])
  when(loader.loadFromConfig()).thenReturn(commandRegistryMaps)

  val commandPluginRegistry = new CommandPluginRegistry(loader)
  def createCommandExecutor(): CommandExecutor = {
    val engine = mock[EngineImpl]
    val commandStorage = new FakeCommandStorage
    val contextFactory = mock[SparkContextFactory]
    val sc = mock[SparkContext]
    when(contextFactory.context(anyString(), Some(anyString()))(any[Invocation])).thenReturn(sc)

    new CommandExecutor(engine, commandStorage)
  }

  //  "create spark context" should "add a entry in command id and context mapping" in {
  //    val args = QuantileValues(List())
  //    var contextCountDuringExecution = 0
  //    var containsKey1DuringExecution = false
  //    val executor = createCommandExecutor()
  //
  //    val dummyFunc = (dist: QuantileValues, user: UserPrincipal, invocation: SparkInvocation) => {
  //      invocation.sparkContext.getConf
  //      contextCountDuringExecution = executor.commandIdContextMapping.size
  //      containsKey1DuringExecution = executor.commandIdContextMapping.contains(1)
  //      mock[FrameEntity]
  //    }
  //
  //    commandPluginRegistry.registerCommand("dummy", dummyFunc)
  //    val user = mock[UserPrincipal]
  //    implicit val call = Call(user)
  //    val execution = executor.execute(CommandTemplate(name = "dummy", arguments = Some(args.toJson.asJsObject())),
  //      commandPluginRegistry)
  //    Await.ready(execution.end, 10 seconds)
  //    contextCountDuringExecution shouldBe 1
  //    containsKey1DuringExecution shouldBe true
  //
  //    //make sure the entry is cleaned up after execution
  //    executor.commandIdContextMapping.size shouldBe 0
  //  }

  //  "cancel command during execution" should "remove the entry from command id and context mapping" in {
  //    val args = QuantileValues(List())
  //    val executor = createCommandExecutor()
  //
  //    var contextCountAfterCancel = 0
  //    val dummyFunc = (dist: QuantileValues, user: UserPrincipal, invocation: SparkInvocation) => {
  //      executor.stopCommand(1)
  //      contextCountAfterCancel = executor.commandIdContextMapping.size
  //      mock[FrameEntity]
  //    }
  //
  //    commandPluginRegistry.registerCommand("dummy", dummyFunc)
  //    val user = mock[UserPrincipal]
  //    implicit val call = Call(user)
  //
  //    val execution = executor.execute(CommandTemplate(name = "dummy", arguments = Some(args.toJson.asJsObject())),
  //      commandPluginRegistry)
  //    Await.ready(execution.end, 10 seconds)
  //    contextCountAfterCancel shouldBe 0
  //  }

}
