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

import com.intel.intelanalytics.domain.frame.{ FrameEntityType, FrameReference, FrameReferenceManagement }
import com.intel.intelanalytics.domain.graph.{ GraphEntityType, GraphReference, GraphReferenceManagement }
import com.intel.intelanalytics.engine.plugin.{ Call, Invocation }
import com.intel.intelanalytics.engine.{ EntityTypeRegistry, ReferenceResolver }
import org.scalatest.{ FlatSpec, Matchers }

import scala.util.Success

class UriReferenceTest extends FlatSpec with Matchers {
  implicit val invocation: Invocation = Call(null, null) //SERBAN - FIX THIS Call(null)

  "A frame uri" should "fail to resolve when no resolvers are registered" in {
    val uri: String = "ia://frame/1"
    val expected = new FrameReference(1)
    val resolver = new EntityTypeRegistry().resolver
    intercept[IllegalArgumentException] {
      resolver.resolve[FrameReference](uri).get
    }
  }

  it should "fail to resolve when no resolver is registered for frames" in {
    val uri: String = "ia://frame/1"
    val expected = new FrameReference(1)
    val registry: EntityTypeRegistry = new EntityTypeRegistry()
    registry.register(GraphEntityType, GraphReferenceManagement)
    val resolver = registry.resolver
    intercept[IllegalArgumentException] {
      resolver.resolve[FrameReference](uri).get
    }
  }

  it should "resolve to a frame when the URI is correct and a resolver is registered" in {
    val uri: String = "ia://frame/1"
    val expected = Success(new FrameReference(1))
    val registry: EntityTypeRegistry = new EntityTypeRegistry()
    registry.register(GraphEntityType, GraphReferenceManagement)
    registry.register(FrameEntityType, FrameReferenceManagement)
    val resolver = registry.resolver
    resolver.resolve[FrameReference](uri) should be(expected)
  }

  it should "be recognized as a valid URI format when the URI is correct and a resolver is registered" in {
    val uri: String = "ia://frame/1"
    val registry: EntityTypeRegistry = new EntityTypeRegistry()
    registry.register(FrameEntityType, FrameReferenceManagement)
    val resolver = registry.resolver
    resolver.isReferenceUriFormat(uri) should be(true)
  }

  it should "not be recognized as a valid URI format when the URI is incorrect" in {
    val uri: String = "not an URI at all"
    val registry: EntityTypeRegistry = new EntityTypeRegistry()
    registry.register(FrameEntityType, FrameReferenceManagement)
    val resolver = registry.resolver
    resolver.isReferenceUriFormat(uri) should be(false)
  }

  "A frame's ia_uri" should "create FrameReference" in {
    val uri: String = "ia://frame/2"
    val expected = Success(new FrameReference(2))

    ReferenceResolver.resolve[FrameReference](uri) should be(expected)
  }

  "A graph's ia_uri and entityName" should "create GraphReference" in {
    val uri: String = "ia://graph/1"
    val expected = Success(new GraphReference(1))

    ReferenceResolver.resolve[FrameReference](uri) should be(expected)
  }

  "A graph's ia_uri" should "create GraphReference" in {
    val uri: String = "ia://graph/2"
    val expected = Success(new GraphReference(2))

    ReferenceResolver.resolve[FrameReference](uri) should be(expected)
  }

  "Incorrect uri" should "throw IllegalArgumentException" in {
    val uri: String = "ia://notaframe/2"
    intercept[IllegalArgumentException] { ReferenceResolver.resolve[FrameReference](uri).get }
  }

}
