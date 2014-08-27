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

package com.intel.intelanalytics.service.v1.decorators

import org.scalatest.{ Matchers, FlatSpec }
import com.intel.intelanalytics.service.v1.viewmodels.RelLink
import com.intel.intelanalytics.domain.frame.DataFrame
import org.joda.time.DateTime
import com.intel.intelanalytics.domain.schema.Schema

class FrameDecoratorTest extends FlatSpec with Matchers {

  val baseUri = "http://www.example.com/dataframes"
  val uri = baseUri + "/1"
  val frame = new DataFrame(1, "name", None, Schema(), 0L, 1L, new DateTime, new DateTime)

  "FrameDecorator" should "be able to decorate a frame" in {
    val decoratedFrame = FrameDecorator.decorateEntity(uri, Nil, frame)
    decoratedFrame.id should be(1)
    decoratedFrame.name should be("name")
    decoratedFrame.ia_uri should be("ia://dataframes/1")
    decoratedFrame.links.head.uri should be("http://www.example.com/dataframes/1")
  }

  it should "set the correct URL in decorating a list of frames" in {
    val frameHeaders = FrameDecorator.decorateForIndex(baseUri, Seq(frame))
    val frameHeader = frameHeaders.toList.head
    frameHeader.url should be("http://www.example.com/dataframes/1")
  }

  it should "add a RelLink for error frames" in {
    val decoratedFrame = FrameDecorator.decorateEntity(uri, Nil, frame.copy(errorFrameId = Some(5)))

    decoratedFrame.links.size should be(2)

    // error frame link
    decoratedFrame.links.head.rel should be("ia-error-frame")
    decoratedFrame.links.head.uri should be("http://www.example.com/dataframes/5")

    // self link
    decoratedFrame.links.last.uri should be("http://www.example.com/dataframes/1")
  }
}
