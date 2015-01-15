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
import com.intel.intelanalytics.engine.EntityTypeRegistry
import com.intel.intelanalytics.engine.plugin.Invocation
import scala.reflect.runtime.{ universe => ru }
import ru._

case class GraphReference(graphId: Long) extends UriReference {
  /** The entity type */
  override def entityType: EntityType = GraphEntityType

  /** The entity id */
  override def id: Long = graphId
}

/**
 * Place to store type tag for graph reference.
 *
 * The same code in GraphEntity had typeTag returning null, presumably
 * due to initialization order issues of some kind. Keeping it in a separate
 * object avoids that problem.
 */
private object GraphTag {
  val referenceTag = typeTag[GraphReference]
}

object GraphEntityType extends EntityType {

  override type Reference = GraphReference

  override implicit val referenceTag: TypeTag[Reference] = GraphTag.referenceTag

  def name = EntityName("graph", "graphs")
}

object GraphReferenceManagement extends EntityManager[GraphEntityType.type] { self =>

  override implicit val referenceTag = GraphEntityType.referenceTag

  //Default resolver that simply creates a reference, with no guarantee that it is valid.
  EntityTypeRegistry.register(GraphEntityType, this)

  override type MetaData = Reference with NoMetaData

  override def getData(reference: Reference)(implicit invocation: Invocation): Data = ???

  override def getMetaData(reference: Reference)(implicit invocation: Invocation): MetaData = ???

  override def create()(implicit invocation: Invocation): Reference = ???

  override def getReference(id: Long)(implicit invocation: Invocation): Reference = new GraphReference(id)

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  override def delete(reference: GraphReferenceManagement.Reference)(implicit invocation: Invocation): Unit = ???

  override type Data = Reference with NoData

  /**
   * Save data of the given type, possibly creating a new object.
   */
  override def saveData(data: Data)(implicit invocation: Invocation): Data = ???
}
