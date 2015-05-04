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

import com.intel.intelanalytics.domain.frame.{ FrameEntityType, FrameReference, FrameReferenceManagement }
import com.intel.intelanalytics.domain.graph.{ GraphEntityType, GraphReference, GraphReferenceManagement }
import com.intel.intelanalytics.engine.plugin.{ Call, Invocation }
import com.intel.intelanalytics.engine.{ EntityTypeRegistry, ReferenceResolver }
import org.scalatest.{ FlatSpec, Matchers }

import scala.util.Success

class UriReferenceTest extends FlatSpec with Matchers {
  implicit val invocation: Invocation = Call(null)

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
