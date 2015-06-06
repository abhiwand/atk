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

package com.intel.intelanalytics.engine

import com.intel.intelanalytics.{ NotNothing, DefaultsTo }
import com.intel.intelanalytics.domain.{ CreateEntityArgs, UriReference, HasData }
import com.intel.intelanalytics.engine.plugin.Invocation

import scala.util.Try
import scala.reflect.runtime.{ universe => ru }
import ru._

case class AugmentedResolver(base: ReferenceResolver, data: Seq[UriReference with HasData]) extends ReferenceResolver {
  /**
   * Returns a reference for the given URI if possible.
   *
   * @throws IllegalArgumentException if no suitable resolver can be found for the entity type in the URI.
   *                                  Note this exception will be in the Try, not actually thrown immediately.
   */
  override def resolve[T <: UriReference: TypeTag](uri: String)(implicit invocation: Invocation,
                                                                e: T DefaultsTo UriReference): Try[T] = {
    require(invocation != null, "invocation is required")
    base.resolve(uri).map { ref =>
      //TODO: this could fail if URIs are not normalized
      val resolved = data.find(d => d.uri == ref.uri).getOrElse(ref)
      ReferenceResolver.coerceReference(resolved)
    }
  }

  /**
   * Checks to see if this string might be a valid reference, without actually trying to resolve it.
   */
  override def isReferenceUriFormat(s: String): Boolean = base.isReferenceUriFormat(s)

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  override def create[T <: UriReference: ru.TypeTag](args: CreateEntityArgs)(implicit invocation: Invocation, ev: NotNothing[T]): T =
    base.create(args)

  /**
   * Creates an (empty) instance of the given type, reserving a URI
   */
  override def delete[T <: UriReference: ru.TypeTag](reference: T)(implicit invocation: Invocation, ev: NotNothing[T]): Unit =
    base.delete(reference)

  def ++(moreData: Seq[UriReference with HasData]) = this.copy(data = this.data ++ moreData)

  /**
   * Save data of the given type, possibly creating a new object.
   */
  override def saveData[T <: UriReference with HasData: ru.TypeTag](data: T)(implicit invocation: Invocation): T = base.saveData(data)
}
