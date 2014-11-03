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

import com.intel.intelanalytics.domain.frame.{ FrameMeta, FrameReference, FrameReferenceManagement, FrameEntity }
import com.intel.intelanalytics.domain.graph.GraphEntity
import com.intel.intelanalytics.engine.plugin.Invocation
import org.scalatest.{ FlatSpec, Matchers }

class EntityRegistryTest extends FlatSpec with Matchers {

  implicit val invocation: Invocation = null

  "Adding a second manager for the same entity" should "replace the original" in {
    val registry = new EntityRegistry
    registry.register(FrameEntity, FrameReferenceManagement)
    registry.register(FrameEntity, new MockFrameManager)

    val data: MockFrameManager#D = registry.resolver.resolve[MockFrameManager#D]("ia://frames/34").get

    data should not be (null)
  }

  "Create" should "create an instance of the right type" in {
    val registry = new EntityRegistry
    registry.register(FrameEntity, FrameReferenceManagement)
    registry.register(FrameEntity, new MockFrameManager)
    registry.register(GraphEntity, new MockGraphManager)

    val data: FrameReference = registry.create[FrameReference]()

    data should not be (null)
  }
  it should "still create an instance of the right type when entities are registered in a different order" in {
    val registry = new EntityRegistry
    registry.register(FrameEntity, FrameReferenceManagement)
    registry.register(GraphEntity, new MockGraphManager)
    registry.register(FrameEntity, new MockFrameManager)

    val data: FrameReference = registry.create[FrameReference]()

    data should not be (null)
  }
  it should "still create an instance of the right type when the requested type is the metadata" in {
    val registry = new EntityRegistry
    registry.register(FrameEntity, new MockFrameManager)
    registry.register(GraphEntity, new MockGraphManager)

    val data: MockFrameManager#M = registry.create[MockFrameManager#M]()

    data should not be (null)
  }

  //  "Register" should "prevent entity managers from being registered for entities they don't manage" in {
  //    val registry = new EntityRegistry
  //    registry.register(FrameEntity, new MockGraphManager)
  //
  //    val data: MockFrameManager#M = registry.create[MockFrameManager#M]()
  //
  //    data should not be (null)
  //  }
}
