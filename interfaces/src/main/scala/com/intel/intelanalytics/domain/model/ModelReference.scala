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

package com.intel.intelanalytics.domain.model

import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.engine.EntityTypeRegistry
import com.intel.intelanalytics.engine.plugin.Invocation

import scala.reflect.runtime.{ universe => ru }
import ru._

trait ModelRef extends UriReference {

  override def entityType: EntityType = ModelEntityType

}

case class ModelReferenceImpl(override val id: Long) extends ModelRef

trait ModelInfo extends ModelRef {

  /**  The entity subtype, like "logistic_regression" */
  //def entitySubtype: String =
}

//case class ModelInfoImpl() extends ModelInfo

case class MyBrickInfo(override val id: Long, color: String, weight: Long) extends ModelInfo {
  //require(ModelReferenceManagement.getMetaData(ModelReferenceManagement.getReference(id)).entitySubtype == "my_brick")

  //override def entitySubtype: String = "my_brick"
}

/**
 * ModelReference is the model's unique identifier. It is used to generate the ia_uri for the model.
 */
case class ModelReference(modelId: Long) extends UriReference {

  /** The entity type */
  override def entityType: EntityType = ModelEntityType

  /** The entity id */
  override def id: Long = modelId
}

/**
 * Place to store type tag for model reference.
 *
 * The same code in FrameEntity had typeTag returning null, presumably
 * due to initialization order issues of some kind. Keeping it in a separate
 * object avoids that problem.
 */
private object ModelTag {
  val referenceTag = typeTag[ModelReference]
}

object ModelEntityType extends EntityType {

  override type Reference = ModelReference

  override implicit val referenceTag: TypeTag[ModelReference] = {
    val tag = ModelTag.referenceTag
    require(tag != null)
    tag
  }

  def name = EntityName("model", "models")
}

object ModelReferenceManagement extends EntityManager[ModelEntityType.type] { self =>

  override implicit val referenceTag = ModelEntityType.referenceTag

  //Default resolver that simply creates a reference, with no guarantee that it is valid.
  EntityTypeRegistry.register(ModelEntityType, this)

  override type MetaData = ModelReference with NoMetaData

  override def getData(reference: Reference)(implicit invocation: Invocation): Data = ???

  override def getMetaData(reference: Reference)(implicit invocation: Invocation): MetaData = ???

  override def create(args: CreateEntityArgs)(implicit invocation: Invocation): Reference = ???

  override def getReference(id: Long)(implicit invocation: Invocation): Reference = new ModelReference(id)

  override type Data = ModelReference with NoData

  /**
   * Save data of the given type, possibly creating a new object.
   */
  override def saveData(data: Data)(implicit invocation: Invocation): Data = ???

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  override def delete(reference: ModelReferenceManagement.Reference)(implicit invocation: Invocation): Unit = ???
}

