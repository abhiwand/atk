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

package com.intel.intelanalytics.service

import org.scalatest.{ Matchers, FlatSpec }

class UrlParserSpec extends FlatSpec with Matchers {

  "UrlParser" should "be able to parse graphIds from graph URI's" in {
    val uri = "http://example.com/v1/graphs/34"
    UrlParser.getGraphId(uri) should be(Some(34))
  }

  it should "be able to parse frameIds from frame URI's" in {
    val uri = "http://example.com/v1/dataframes/55"
    UrlParser.getFrameId(uri) should be(Some(55))
  }

  it should "NOT parse frameIds from invalid URI's" in {
    val uri = "http://example.com/v1/invalid/55"
    UrlParser.getFrameId(uri) should be(None)
  }

  it should "NOT parse non-numeric frame ids" in {
    val uri = "http://example.com/v1/invalid/ABC"
    UrlParser.getFrameId(uri) should be(None)
  }
}
