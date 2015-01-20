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

package com.intel.intelanalytics.domain

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
 * Examples include [[com.intel.intelanalytics.domain.graph.Graph]] and
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
