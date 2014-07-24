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

package com.intel.intelanalytics.service.v1.viewmodels

import org.scalatest.{ Matchers, FlatSpec }

class RelLinkSpec extends FlatSpec with Matchers {

  "RelLink" should "be able to create a self link" in {

    val uri = "http://www.example.com/"
    val relLink = Rel.self(uri)

    relLink.rel should be("self")
    relLink.method should be("GET")
    relLink.uri should be(uri)
  }

  it should "not allow invalid methods" in {
    try {
      RelLink("ia-foo", "uri", "WHACK")
      fail("expected exception not thrown")
    }
    catch {
      case e: IllegalArgumentException => // pass
      case _: Exception => fail("expected exception not thrown")
    }

  }
}
