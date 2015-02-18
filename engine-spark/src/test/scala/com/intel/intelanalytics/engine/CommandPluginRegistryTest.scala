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

package com.intel.intelanalytics.engine

import org.scalatest.{ Matchers, FlatSpec }
import org.mockito.Mockito._
import com.intel.intelanalytics.engine.spark.command.{ CommandPluginRegistry, CommandLoader }
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

    val dummyFunc = (dist: CumulativeSumArgs, user: UserPrincipal, invocation: SparkInvocation) => {
      mock[FrameEntity]
    }

    val plugin = registry.registerCommand("dummy", dummyFunc)
    registry.getCommandPlugin("dummy") shouldBe Some(plugin)

  }
}
