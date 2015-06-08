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
