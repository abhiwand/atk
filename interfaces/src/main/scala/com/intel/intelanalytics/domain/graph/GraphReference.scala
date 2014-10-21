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

package com.intel.intelanalytics.domain.graph

import com.intel.intelanalytics.domain._

case class GraphReference(graphId: Long, graphExists: Option[Boolean] = None) extends UriReference {
  /** The entity type */
  override def entity: Entity = GraphReferenceManagement

  /** The entity id */
  override def id: Long = graphId

  /**
   * Is this reference known to be valid at the time it was created?
   *
   * None indicates this is unknown.
   */
  override def exists: Option[Boolean] = graphExists

}

object GraphReferenceManagement extends EntityManagement { self =>

  //Default resolver that simply creates a reference, with no guarantee that it is valid.
  EntityRegistry.register[self.type](this)

  def name = EntityName("graph", "graphs")

  def apply(graphId: Long, graphExists: Option[Boolean] = None) = new GraphReference(graphId, graphExists)

  override type MetaData = Reference with NoMetaData

  override def getData(reference: Reference): Data = ???

  override def getMetaData(reference: Reference): MetaData = ???

  override def create(): Reference = ???

  override def getReference(id: Long): Reference = new GraphReference(id, None)

  override type Data = Reference with NoData

  override type Reference = GraphReference
}
