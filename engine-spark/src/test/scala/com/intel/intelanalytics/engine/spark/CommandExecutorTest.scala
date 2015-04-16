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

package com.intel.intelanalytics.engine.spark

import com.intel.intelanalytics.engine.{ ProgressInfo, CommandStorage }
import org.joda.time.DateTime
import org.scalatest.{ Matchers, FlatSpec }
import com.intel.intelanalytics.engine.spark.command._
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.intel.intelanalytics.domain.command.{ Command, CommandTemplate }
import scala.concurrent.duration._
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import org.apache.spark.SparkContext
import scala.concurrent.ExecutionContext.Implicits.global
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation
import com.intel.intelanalytics.domain.frame.{ CumulativeSumArgs, FrameEntity }
import com.intel.intelanalytics.domain.frame.QuantileValues
import scala.concurrent.{ Await, ExecutionContext }
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol
import DomainJsonProtocol._
import com.intel.intelanalytics.engine.plugin.{ Call, Invocation, CommandPlugin }
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
    val engine = mock[SparkEngine]
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
