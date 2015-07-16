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

package com.intel.taproot.analytics.engine

import com.intel.taproot.analytics.domain.{ CreateEntityArgs, HasData, HasMetaData, EntityManager }
import com.intel.taproot.analytics.domain.graph.{ GraphReference, GraphEntityType }
import com.intel.taproot.analytics.engine.Rows._
import com.intel.taproot.analytics.engine.plugin.Invocation

class MockGraphManager extends EntityManager[GraphEntityType.type] {

  override implicit val referenceTag = GraphEntityType.referenceTag
  require(referenceTag != null)

  class M(id: Long) extends GraphReference(id) with HasMetaData {
    override type Meta = Int
    override val meta = 3
  }
  class D(id: Long) extends M(id) with HasData {
    override type Data = Seq[Row]
    override val data = Seq(
      Array[Any](1, 2, 3),
      Array[Any](4, 5, 6)
    )
  }
  override type MetaData = M

  override def getReference(id: Long)(implicit invocation: Invocation): Reference = new GraphReference(id)

  override type Data = D

}
