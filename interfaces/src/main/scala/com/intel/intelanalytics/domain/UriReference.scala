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

package com.intel.intelanalytics.domain

import java.net.URI

import com.intel.intelanalytics.engine.{ ReferenceResolver, EntityTypeRegistry }
import com.intel.intelanalytics.engine.plugin.Invocation

import scala.util.Try

import scala.reflect.runtime.{ universe => ru }
import ru._

/**
 * Things that can be referenced with a simple URI of the form scheme://entity/id.
 */
trait UriReference extends HasId {

  /** URI scheme. Default is "ia" */
  def scheme: String = "ia"

  /** The entity id */
  def id: Long

  /** The entity name */
  def name: String = null

  /** The entity type */
  def entityType: EntityType

  /** The full URI */
  def uri: String = {
    val ia_uri: String = s"$scheme://${entityType.name.singular}/$id"
    ia_uri
  }

  override def hashCode(): Int = uri.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: UriReference => this.uri == x.uri
    case _ => false
  }
}
