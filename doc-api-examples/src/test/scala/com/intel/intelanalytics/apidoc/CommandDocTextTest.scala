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

package com.intel.intelanalytics.apidoc

import org.scalatest.{ FlatSpec, Matchers }

class CommandDocTextTest extends FlatSpec with Matchers {

  "getDocText" should "return None if resource file not found" in {
    CommandDocText.getText("entity/bogus", "test") should be(None)
  }

  "getDocText" should "find resource file and return contents" in {
    val doc = CommandDocText.getText("entity/function", "test")
    doc should not be None
    doc.get should be("""    One line summary with period.

    Extended Summary
    ----------------
    Extended summary about the function which may be
    multiple lines

    Parameters
    ----------
    arg1 : str
        The description of arg1
    arg2 : int
        The description of arg2

    Returns
    -------
    result : str
        The description of the result of the function

    Examples
    --------
    Have some really good examples here. And end paragraphs
    with a double colon to start 'code' sections::

    frame.function("a", 3)

    "aaa"

""")
    println(doc)
  }
}
