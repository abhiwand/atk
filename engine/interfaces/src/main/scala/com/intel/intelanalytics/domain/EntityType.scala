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

package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.graph.GraphReferenceManagement._
import com.intel.intelanalytics.engine.plugin.Invocation
import scala.reflect.runtime.{ universe => ru }
import ru._

/**
 * Describes the structure of a name, including both singular and plural,
 * for use in constructing URIs and other identifiers for entities in the system.
 * @param singular the singular name for the entity
 * @param plural the plural name for the entity
 */
case class EntityName(singular: String, plural: String)

/**
 * Indicates that this reference has attached metadata
 */
trait HasMetaData {
  type Meta
  def meta: Meta
}

/**
 * Useful in testing
 */
class NoMetaData extends HasMetaData {
  override type Meta = Unit
  val meta = ()
}

/**
 * Indicates that a reference includes attached metadata and data
 */
trait HasData extends HasMetaData {
  type Data
  def data: Data
}

/**
 * Useful in testing
 */
class NoData extends NoMetaData with HasData {
  override type Data = Unit
  val data = ()
}

/**
 * Entities in the system, things which can be named in URIs and other identifiers.
 *
 * Examples include [[com.intel.intelanalytics.domain.graph.GraphEntity]] and
 * [[com.intel.intelanalytics.domain.frame.FrameEntity]].
 */
trait EntityType {

  /**
   * The type of the UriReference that is used for this entity. Note that
   * no other Entity should use the same Reference type.
   */
  type Reference <: UriReference

  def referenceTag: TypeTag[Reference]

  /**
   * The standard name for this entity
   */
  def name: EntityName

  /**
   * Other names that are also recognized / accepted in incoming requests,
   * but not generally used for constructing outgoing responses.
   */
  def alternatives: Seq[EntityName] = Seq()

}

/**
 * Additional functionality for Entities to allow creation (of placeholders) and retrieval of entities'
 * metadata and data.
 */
trait EntityManager[E <: EntityType] { self =>

  type Reference = E#Reference

  implicit def referenceTag: TypeTag[Reference]

  type MetaData <: Reference with HasMetaData

  type Data <: MetaData with HasData

  def create(args: CreateEntityArgs)(implicit invocation: Invocation): Reference

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  def delete(reference: Reference)(implicit invocation: Invocation): Unit

  def getReference(id: Long)(implicit invocation: Invocation): Reference

  def getMetaData(reference: Reference)(implicit invocation: Invocation): MetaData

  def getData(reference: Reference)(implicit invocation: Invocation): Data

  /**
   * Save data of the given type, possibly creating a new object.
   */
  def saveData(data: Data)(implicit invocation: Invocation): Data

}
