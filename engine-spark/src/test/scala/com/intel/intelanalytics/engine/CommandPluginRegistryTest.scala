package com.intel.intelanalytics.engine

import org.scalatest.{ Matchers, FlatSpec }
import org.mockito.Mockito._
import com.intel.intelanalytics.engine.spark.command.{ CommandPluginRegistry, CommandLoader }
import com.intel.intelanalytics.engine.plugin.CommandPlugin
import scala.collection.immutable.HashMap
import com.intel.intelanalytics.domain.frame.{ DataFrame, CumulativeSum }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.engine.spark.plugin.SparkInvocation

import com.intel.intelanalytics.domain.DomainJsonProtocol
import DomainJsonProtocol._
import org.scalatest.mock.MockitoSugar

class CommandPluginRegistryTest extends FlatSpec with Matchers with MockitoSugar {
  "plugin registry initialization" should "load from the loader" in {
    val loader = mock[CommandLoader]
    val mockPlugin = mock[CommandPlugin[Product, Product]]
    when(loader.loadFromConfig()).thenReturn(new HashMap[String, CommandPlugin[_, _]] + ("mock-plugin" -> mockPlugin))
    val registry = new CommandPluginRegistry(loader)
    registry.getCommandPlugin("mock-plugin") shouldBe Some(mockPlugin)
    registry.getCommandPlugin("not exists") shouldBe None
  }

  "registry plugin" should "add to the registry" in {
    val loader = mock[CommandLoader]
    val mockPlugin = mock[CommandPlugin[Product, Product]]
    when(loader.loadFromConfig()).thenReturn(new HashMap[String, CommandPlugin[_, _]] + ("mock-plugin" -> mockPlugin))
    val registry = new CommandPluginRegistry(loader)

    val dummyFunc = (dist: CumulativeSum, user: UserPrincipal, invocation: SparkInvocation) => {
      mock[DataFrame]
    }

    val plugin = registry.registerCommand("dummy", dummyFunc)
    registry.getCommandPlugin("dummy") shouldBe Some(plugin)

  }
}
