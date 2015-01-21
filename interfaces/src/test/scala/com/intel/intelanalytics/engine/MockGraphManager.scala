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

import com.intel.intelanalytics.domain.{ CreateEntityArgs, HasData, HasMetaData, EntityManager }
import com.intel.intelanalytics.domain.graph.{ GraphReference, GraphEntityType }
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.Invocation

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

  override def getData(reference: Reference)(implicit invocation: Invocation): Data = new D(reference.id)

  override def getMetaData(reference: Reference)(implicit invocation: Invocation): MetaData = new M(reference.id)

  override def create(args: CreateEntityArgs)(implicit invocation: Invocation): Reference = ???

  override def getReference(id: Long)(implicit invocation: Invocation): Reference = new GraphReference(id)

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
