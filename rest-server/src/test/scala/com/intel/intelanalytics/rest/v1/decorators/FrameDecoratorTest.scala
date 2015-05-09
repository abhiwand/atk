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

package com.intel.intelanalytics.rest.v1.decorators

import com.intel.intelanalytics.domain.frame.FrameEntity
import com.intel.intelanalytics.domain.schema.Schema
import org.joda.time.DateTime
import org.scalatest.{ FlatSpec, Matchers }
import org.joda.time.DateTime
import com.intel.intelanalytics.domain.schema.{ FrameSchema, Schema }

class FrameDecoratorTest extends FlatSpec with Matchers {

  val baseUri = "http://www.example.com/frames"
  val uri = baseUri + "/1"
  val frame = new FrameEntity(1, Some("name"), FrameSchema(), 1L, new DateTime, new DateTime)

  "FrameDecorator" should "be able to decorate a frame" in {
    val decoratedFrame = FrameDecorator.decorateEntity(uri, Nil, frame)
    decoratedFrame.id should be(1)
    decoratedFrame.name should be(Some("name"))
    decoratedFrame.ia_uri should be("ia://frame/1")
    decoratedFrame.entityType should be("frame:")
    decoratedFrame.links.head.uri should be("http://www.example.com/frames/1")
  }

  it should "set the correct URL in decorating a list of frames" in {
    val frameHeaders = FrameDecorator.decorateForIndex(baseUri, Seq(frame))
    val frameHeader = frameHeaders.toList.head
    frameHeader.url should be("http://www.example.com/frames/1")
  }

  it should "add a RelLink for error frames" in {
    val decoratedFrame = FrameDecorator.decorateEntity(uri, Nil, frame.copy(errorFrameId = Some(5)))

    decoratedFrame.links.size should be(2)

    // error frame link
    decoratedFrame.links.head.rel should be("ia-error-frame")
    decoratedFrame.links.head.uri should be("http://www.example.com/frames/5")

    // self link
    decoratedFrame.links.last.uri should be("http://www.example.com/frames/1")
  }
}