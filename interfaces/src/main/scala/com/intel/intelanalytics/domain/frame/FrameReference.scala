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

package com.intel.intelanalytics.domain.frame

import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.engine.EntityRegistry
import com.intel.intelanalytics.engine.plugin.Invocation
import scala.reflect.runtime.{ universe => ru }
import ru._

case class FrameReference(frameId: Long, frameExists: Option[Boolean] = None) extends UriReference {

  /** The entity type */
  override def entity: EntityType = FrameEntity

  /** The entity id */
  override def id: Long = frameId

  /**
   * Is this reference known to be valid at the time it was created?
   *
   * None indicates this is unknown.
   */
  override def exists: Option[Boolean] = frameExists
}

/**
 * Place to store type tag for frame reference.
 *
 * The same code in FrameEntity had typeTag returning null, presumably
 * due to initialization order issues of some kind. Keeping it in a separate
 * object avoids that problem.
 */
private object FrameTag {
  val referenceTag = typeTag[FrameReference]
}

object FrameEntity extends EntityType {

  override type Reference = FrameReference

  override implicit val referenceTag: TypeTag[FrameReference] = {
    val tag = FrameTag.referenceTag
    require(tag != null)
    tag
  }

  def name = EntityName("frame", "frames")

  override def alternatives = Seq(EntityName("dataframe", "dataframes"))

  def apply(frameId: Long, frameExists: Option[Boolean]) = new FrameReference(frameId, frameExists)

}

object FrameReferenceManagement extends EntityManager[FrameEntity.type] { self =>

  override implicit val referenceTag = FrameEntity.referenceTag

  //Default resolver that simply creates a reference, with no guarantee that it is valid.
  EntityRegistry.register(FrameEntity, this)

  override type MetaData = FrameReference with NoMetaData

  override def getData(reference: Reference)(implicit invocation: Invocation): Data = ???

  override def getMetaData(reference: Reference): MetaData = ???

  override def create()(implicit invocation: Invocation): Reference = ???

  override def getReference(id: Long): Reference = new FrameReference(id, None)

  override type Data = FrameReference with NoData

  /**
   * Save data of the given type, possibly creating a new object.
   */
  override def saveData(data: Data)(implicit invocation: Invocation): Data = ???
}
