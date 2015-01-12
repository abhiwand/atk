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

import java.net.URI

import com.intel.intelanalytics.{ NotNothing, DefaultsTo }
import com.intel.intelanalytics.domain.{ UriReference, HasData, HasMetaData }
import com.intel.intelanalytics.engine.plugin.Invocation

import scala.util.Try
import scala.reflect.runtime.{ universe => ru }
import ru._

/**
 * Provides a way to get access to arbitrary objects in the system by using an URI.
 *
 * The methods in this class are thread safe.
 *
 * Note that this class is generally used through the companion object.
 */
class RegistryReferenceResolver(registry: EntityTypeRegistry) extends ReferenceResolver {

  var resolvers: Map[String, (Long, Invocation) => UriReference] = registry.entities.flatMap {
    case (entity, manager) =>
      val resolver: (Long, Invocation) => UriReference = (id: Long, invocation: Invocation) => manager.getReference(id)(invocation)
      (Seq(entity.name) ++ entity.alternatives).flatMap { name =>
        Seq(name.singular -> resolver,
          name.plural -> resolver)
      }
  }.toMap[String, (Long, Invocation) => UriReference]

  val regex = resolvers.keys match {
    case keys if keys.isEmpty => "<invalid>".r
    case keys => keys.map(k => s"""($k/[0-9]+)""").mkString(""".+/(""", "|", ")").r
  }

  /**
   * Checks to see if this string might be a valid reference, without actually trying to resolve it.
   */
  def isReferenceUriFormat(s: String) = regex.findFirstIn(s).isDefined match {
    case true => true
    case false =>
      println(s"url $s not matched with $regex")
      false
  }

  /**
   * Returns a reference for the given URI if possible.
   *
   * @throws IllegalArgumentException if no suitable resolver can be found for the entity type in the URI.
   *                                  Note this exception will be in the Try, not actually thrown immediately.
   */
  def resolve[T <: UriReference: TypeTag](uri: String)(implicit invocation: Invocation,
                                                       e: T DefaultsTo UriReference): Try[T] = Try {
    require(invocation != null, "invocation is required")
    new URI(uri) //validate this is actually a URI at all
    val regexMatch = regex.findFirstMatchIn(uri)
      .getOrElse(throw new IllegalArgumentException("Could not find entity name in " + uri))

    //Error should never happen on next line, since our regex includes subgroups -
    //a match means there is at least one subgroup match as well.
    val matched = regexMatch.subgroups.find(s => s != null).getOrElse(throw new Exception("Internal error"))

    val parts = matched.split("/")
    if (parts.length < 2) {
      throw new IllegalArgumentException("No valid entity found in " + uri)
    }
    val entity = parts(0)
    val id = Try {
      parts(1).toLong
    }.getOrElse(
      throw new IllegalArgumentException(s"Could not parse entity ID in '${regexMatch.toString()}' of '$uri'"))
    val resolver = resolvers.getOrElse(entity,
      throw new IllegalArgumentException(s"No resolver found for entity: $entity"))

    val uriReference = resolver(id, invocation)

    val manager = registry.entityManager(uriReference.entityType).getOrElse(
      throw new IllegalArgumentException(s"No entity manager found for entity type '$entity' (or '$typeTag[T]')"))

    val reference = ReferenceResolver.coerceReference[manager.Reference](uriReference)(manager.referenceTag)
    val detailed = typeTag[T] match {
      case x if x.tpe <:< typeTag[HasData].tpe =>
        manager.getData(reference)
      case x if x.tpe <:< typeTag[HasMetaData].tpe =>
        manager.getMetaData(reference)
      case _ =>
        reference
    }

    ReferenceResolver.coerceReference(detailed)
  }

  def create[T <: UriReference: TypeTag](annotation: Option[String] = None)(implicit invocation: Invocation, ev: NotNothing[T]) = {
    registry.create[T](annotation)
  }

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  override def delete[T <: UriReference: ru.TypeTag](reference: T)(implicit invocation: Invocation, ev: NotNothing[T]): Unit = {
    registry.delete(reference)
  }

  /**
   * Save data of the given type, possibly creating a new object.
   */
  def saveData[T <: UriReference with HasData: TypeTag](data: T)(implicit invocation: Invocation): T = {
    registry.saveData(data)
  }
}
