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

package com.intel.intelanalytics.domain.command
import org.scalatest.{ FlatSpec, Matchers }
import org.scalatest.PrivateMethodTester._

class CommandDocLoaderTest extends FlatSpec with Matchers {

  "createCommandDoc" should "split text into two pieces" in {
    val createCommandDoc = PrivateMethod[Option[CommandDoc]]('createCommandDoc)
    val doc = CommandDocLoader invokePrivate createCommandDoc(
      Some("""    One-liner.

    Line 3
    Line 4"""))
    doc should not be None
    doc.get.oneLineSummary should be("One-liner.")
    doc.get.extendedSummary.get should be("""

    Line 3
    Line 4""")
    println(doc)
  }

  "getPath" should "replace special chars" in {
    val getPath = PrivateMethod[String]('getPath)
    CommandDocLoader invokePrivate getPath("frame:vertex/count") should be("frame-vertex/count")
  }
}
