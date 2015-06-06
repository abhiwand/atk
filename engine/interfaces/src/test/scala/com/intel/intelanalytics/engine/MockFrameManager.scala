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

import com.intel.intelanalytics.domain.{ CreateEntityArgs, HasData, HasMetaData, EntityManager }
import com.intel.intelanalytics.domain.frame.{ FrameReference, FrameEntityType }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.Invocation

class MockFrameManager extends EntityManager[FrameEntityType.type] {

  var id = 0

  override implicit val referenceTag = FrameEntityType.referenceTag
  require(referenceTag != null)

  class M(id: Long) extends FrameReference(id) with HasMetaData {
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

  override def getData(reference: Reference)(implicit invocation: Invocation): Data = new D(reference.id)

  override def getMetaData(reference: Reference)(implicit invocation: Invocation): MetaData = new M(reference.id)

  override def create(args: CreateEntityArgs = null)(implicit invocation: Invocation): Reference = new FrameReference({ id += 1; id })

  override def getReference(id: Long)(implicit invocation: Invocation): Reference = new FrameReference(id)

  override type Data = D

  /**
   * Save data of the given type, possibly creating a new object.
   */
  override def saveData(data: Data)(implicit invocation: Invocation): Data = ???

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  override def delete(reference: Reference)(implicit invocation: Invocation): Unit = ???
}
