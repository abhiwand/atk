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

package com.intel.taproot.analytics.engine

import com.intel.taproot.analytics.{ DefaultsTo, NotNothing }
import com.intel.taproot.analytics.domain.{ CreateEntityArgs, HasData, UriReference }
import com.intel.taproot.analytics.engine.plugin.Invocation

import scala.util.Try
import scala.reflect.runtime.{ universe => ru }
import ru._

/**
 * The default system registry of URI reference resolvers
 */
object ReferenceResolver extends ReferenceResolver {

  private[analytics] def coerceReference[T <: UriReference: TypeTag](ref: UriReference): T = {
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
