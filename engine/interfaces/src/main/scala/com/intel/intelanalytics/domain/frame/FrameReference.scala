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

package com.intel.intelanalytics.domain.frame

import com.intel.intelanalytics.domain._
import com.intel.intelanalytics.engine.EntityTypeRegistry
import com.intel.intelanalytics.engine.plugin.Invocation
import scala.reflect.runtime.{ universe => ru }
import ru._

trait FrameRef extends UriReference {

  override def entityType: EntityType = FrameEntityType

}

case class FrameReference(frameId: Long) extends UriReference {

  /** The entity type */
  override def entityType: EntityType = FrameEntityType

  /** The entity id */
  override def id: Long = frameId
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

object FrameEntityType extends EntityType {

  override type Reference = FrameReference

  override implicit val referenceTag: TypeTag[FrameReference] = {
    val tag = FrameTag.referenceTag
    require(tag != null)
    tag
  }

  def name = EntityName("frame", "frames")

  override def alternatives = Seq(EntityName("dataframe", "dataframes"))
}

object FrameReferenceManagement extends EntityManager[FrameEntityType.type] { self =>

  override implicit val referenceTag = FrameEntityType.referenceTag

  //Default resolver that simply creates a reference, with no guarantee that it is valid.
  EntityTypeRegistry.register(FrameEntityType, this)

  override type MetaData = FrameReference with NoMetaData

  override def getData(reference: Reference)(implicit invocation: Invocation): Data = ???

  override def getMetaData(reference: Reference)(implicit invocation: Invocation): MetaData = ???

  override def create(args: CreateEntityArgs)(implicit invocation: Invocation): Reference = ???

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  override def delete(reference: FrameReferenceManagement.Reference)(implicit invocation: Invocation): Unit = ???

  override def getReference(id: Long)(implicit invocation: Invocation): Reference = new FrameReference(id)

  override type Data = FrameReference with NoData

  /**
   * Save data of the given type, possibly creating a new object.
   */
  override def saveData(data: Data)(implicit invocation: Invocation): Data = ???
}
