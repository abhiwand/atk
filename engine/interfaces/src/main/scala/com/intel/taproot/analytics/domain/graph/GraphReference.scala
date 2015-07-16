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

package com.intel.taproot.analytics.domain.graph

import com.intel.taproot.analytics.domain._
import com.intel.taproot.analytics.engine.EntityTypeRegistry
import com.intel.taproot.analytics.engine.plugin.Invocation
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

  override def getReference(id: Long)(implicit invocation: Invocation): Reference = new GraphReference(id)

  override type Data = Reference with NoData

}
