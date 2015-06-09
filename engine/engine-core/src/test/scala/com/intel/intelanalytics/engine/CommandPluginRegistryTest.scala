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

package com.intel.intelanalytics.engine

import org.scalatest.{ Matchers, FlatSpec }
import org.mockito.Mockito._
import com.intel.intelanalytics.engine.spark.command.{ CommandPluginRegistryMaps, CommandPluginRegistry, CommandLoader }
import com.intel.intelanalytics.engine.plugin.CommandPlugin
import scala.collection.immutable.HashMap
import com.intel.intelanalytics.domain.frame.{ FrameEntity, CumulativeSumArgs }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation

import com.intel.intelanalytics.domain.DomainJsonProtocol
import DomainJsonProtocol._
import org.scalatest.mock.MockitoSugar

class CommandPluginRegistryTest extends FlatSpec with Matchers with MockitoSugar {
  "plugin registry initialization" should "load from the loader" in {
    val loader = mock[CommandLoader]
    val mockPlugin = mock[CommandPlugin[Product, Product]]
    val commandRegistryMaps = CommandPluginRegistryMaps(new HashMap[String, CommandPlugin[_, _]], new HashMap[String, String])
    commandRegistryMaps.commandPlugins += ("mock-plugin" -> mockPlugin)
    when(loader.loadFromConfig()).thenReturn(commandRegistryMaps)
    val registry = new CommandPluginRegistry(loader)
    registry.getCommandPlugin("mock-plugin") shouldBe Some(mockPlugin)
    registry.getCommandPlugin("not exists") shouldBe None
  }

  "registry plugin" should "add to the registry" in {
    val loader = mock[CommandLoader]
    val mockPlugin = mock[CommandPlugin[Product, Product]]
    val commandRegistryMaps = CommandPluginRegistryMaps(new HashMap[String, CommandPlugin[_, _]], new HashMap[String, String])
    commandRegistryMaps.commandPlugins += ("mock-plugin" -> mockPlugin)
    when(loader.loadFromConfig()).thenReturn(commandRegistryMaps)
    val registry = new CommandPluginRegistry(loader)

    val dummyFunc = (dist: CumulativeSumArgs, user: UserPrincipal, invocation: SparkInvocation) => {
      mock[FrameEntity]
    }

    val plugin = registry.registerCommand("dummy", dummyFunc)
    registry.getCommandPlugin("dummy") shouldBe Some(plugin)

  }

  "plugin" should "return archive name" in {
    val loader = mock[CommandLoader]
    val commandRegistryMaps = CommandPluginRegistryMaps(new HashMap[String, CommandPlugin[_, _]], new HashMap[String, String])
    commandRegistryMaps.pluginsToArchiveMap += ("mock-plugin" -> "mock-archive")
    when(loader.loadFromConfig()).thenReturn(commandRegistryMaps)
    val registry = new CommandPluginRegistry(loader)
    registry.getArchiveNameFromPlugin("mock-plugin") shouldBe Some("mock-archive")
    registry.getCommandPlugin("not exists") shouldBe None
  }
}
