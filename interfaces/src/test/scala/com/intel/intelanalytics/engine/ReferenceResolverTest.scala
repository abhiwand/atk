//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

import com.intel.intelanalytics.domain.frame.{ FrameEntity, FrameReference }
import com.intel.intelanalytics.domain.graph.{ GraphEntity, GraphReference }
import com.intel.intelanalytics.engine.plugin.Invocation
import org.scalatest.{ FlatSpec, Matchers }

class ReferenceResolverTest extends FlatSpec with Matchers {

  val registry = new EntityRegistry
  registry.register(FrameEntity, new MockFrameManager)
  registry.register(GraphEntity, new MockGraphManager)
  val resolver = registry.resolver
  implicit val invocation: Invocation = null

  "Reference resolver" should "return metadata when requested" in {
    val meta: MockFrameManager#M = resolver.resolve[MockFrameManager#M]("ia://frames/6").get
    meta should not be (null)
    val gm: MockGraphManager#M = resolver.resolve[MockGraphManager#M]("ia://graphs/6").get
    gm should not be (null)
  }

  it should "return data when requested" in {
    val data: MockFrameManager#D = resolver.resolve[MockFrameManager#D]("ia://frames/6").get

    data should not be (null)
    val gd: MockGraphManager#D = resolver.resolve[MockGraphManager#D]("ia://graphs/6").get
    gd should not be (null)
  }

  it should "return a plain reference when requested" in {
    val ref: FrameReference = resolver.resolve[FrameReference]("ia://frames/6").get

    ref should not be (null)
    val gr: GraphReference = resolver.resolve[GraphReference]("ia://graphs/6").get
    gr should not be (null)
  }

  it should "throw IllegalArgumentException when no entity is registered" in {
    val registry = new EntityRegistry
    registry.register(FrameEntity, new MockFrameManager)
    val resolver = registry.resolver
    intercept[IllegalArgumentException] {
      val gm: MockGraphManager#M = resolver.resolve[MockGraphManager#M]("ia://graphs/6").get
    }
  }

}
