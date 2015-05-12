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

import org.scalatest.FlatSpec
import org.joda.time.DateTime

class UserTest extends FlatSpec {

  "User" should "be able to have a none apiKey" in {
    new User(1L, None, None, new DateTime, new DateTime)
  }

  it should "not be able to have a null apiKey" in {
    intercept[IllegalArgumentException] { new User(1L, None, null, new DateTime, new DateTime) }
  }

  it should "not be able to have an empty string apiKey" in {
    intercept[IllegalArgumentException] { new User(1L, None, Some(""), new DateTime, new DateTime) }
  }

  it should "have id greater than zero" in {
    intercept[IllegalArgumentException] { new User(-1L, None, Some("api"), new DateTime, new DateTime) }
  }
}
