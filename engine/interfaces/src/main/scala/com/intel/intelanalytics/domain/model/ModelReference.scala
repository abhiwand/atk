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
