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

import com.intel.intelanalytics.domain.frame
import com.intel.intelanalytics.domain.frame.{ FrameMeta, FrameEntityType, FrameReference }
import com.intel.intelanalytics.domain.graph.{ GraphEntityType, GraphReference }
import com.intel.intelanalytics.engine.plugin.{ Call, Invocation }
import com.intel.intelanalytics.engine.spark.threading.EngineExecutionContext
import org.scalatest.{ FlatSpec, Matchers }

class ReferenceResolverTest extends FlatSpec with Matchers {

  val registry = new EntityTypeRegistry
  registry.register(FrameEntityType, new MockFrameManager)
  registry.register(GraphEntityType, new MockGraphManager)
  val resolver = registry.resolver
  implicit val invocation: Invocation = Call(null, EngineExecutionContext.global)

  "resolve" should "return metadata when requested" in {
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
    val registry = new EntityTypeRegistry
    registry.register(FrameEntityType, new MockFrameManager)
    val resolver = registry.resolver
    intercept[IllegalArgumentException] {
      val gm: MockGraphManager#M = resolver.resolve[MockGraphManager#M]("ia://graphs/6").get
    }
  }

  "create" should "create a FrameReference when requested" in {
    val ref = resolver.create[FrameReference]()

    ref should not be (null)
    ref should be(a[FrameReference])
  }
  it should "create a meta when requested" in {
    val ref = resolver.create[MockFrameManager#M]()

    ref should not be (null)
    ref should be(a[MockFrameManager#M])
  }
}
