//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

import com.intel.intelanalytics.{ DefaultsTo, NotNothing }
import com.intel.intelanalytics.domain.{ CreateEntityArgs, HasData, UriReference }
import com.intel.intelanalytics.engine.plugin.Invocation

import scala.util.Try
import scala.reflect.runtime.{ universe => ru }
import ru._

/**
 * The default system registry of URI reference resolvers
 */
object ReferenceResolver extends ReferenceResolver {

  private[intelanalytics] def coerceReference[T <: UriReference: TypeTag](ref: UriReference): T = {
    Try {
      ref.asInstanceOf[T]
    }.getOrElse(throw new IllegalArgumentException(s"Could not convert $ref to an instance of $typeTag[T]"))
  }

  /**
   * Returns a reference for the given URI if possible.
   *
   * @throws IllegalArgumentException if no suitable resolver can be found for the entity type in the URI.
   *                                  Note this exception will be in the Try, not actually thrown immediately.
   */
  override def resolve[T <: UriReference: TypeTag](uri: String)(implicit invocation: Invocation,
                                                                e: T DefaultsTo UriReference): Try[T] =
    EntityTypeRegistry.resolver.resolve(uri)

  /**
   * Checks to see if this string might be a valid reference, without actually trying to resolve it.
   */
  override def isReferenceUriFormat(s: String): Boolean = EntityTypeRegistry.resolver.isReferenceUriFormat(s)

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  override def create[T <: UriReference: ru.TypeTag](args: CreateEntityArgs)(implicit invocation: Invocation, ev: NotNothing[T]): T =
    EntityTypeRegistry.resolver.create(args)

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  def delete[T <: UriReference: TypeTag](reference: T)(implicit invocation: Invocation, ev: NotNothing[T]): Unit =
    EntityTypeRegistry.resolver.delete(reference)

  /**
   * Save data of the given type, possibly creating a new object.
   */
  def saveData[T <: UriReference with HasData: TypeTag](data: T)(implicit invocation: Invocation): T = EntityTypeRegistry.resolver.saveData(data)

}

trait ReferenceResolver {
  /**
   * Checks to see if this string might be a valid reference, without actually trying to resolve it.
   */
  def isReferenceUriFormat(s: String): Boolean

  /**
   * Returns a reference for the given URI if possible.
   *
   * @throws IllegalArgumentException if no suitable resolver can be found for the entity type in the URI.
   *                                  Note this exception will be in the Try, not actually thrown immediately.
   */
  def resolve[T <: UriReference: TypeTag](uri: String)(implicit invocation: Invocation,
                                                       e: T DefaultsTo UriReference): Try[T]

  /**
   * Returns a (possibly updated) reference.
   */
  //TODO: Make the return type a subtype of the argument type
  def resolve[T <: UriReference: TypeTag](reference: UriReference)(implicit invocation: Invocation, ev: NotNothing[T]): Try[T] = {
    resolve[T](reference.uri)
  }

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  def create[T <: UriReference: TypeTag](args: CreateEntityArgs = CreateEntityArgs())(implicit invocation: Invocation, ev: NotNothing[T]): T

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  def delete[T <: UriReference: TypeTag](reference: T)(implicit invocation: Invocation, ev: NotNothing[T]): Unit

  /**
   * Save data of the given type, possibly creating a new object.
   */
  def saveData[T <: UriReference with HasData: TypeTag](data: T)(implicit invocation: Invocation): T

}
