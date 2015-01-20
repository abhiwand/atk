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
