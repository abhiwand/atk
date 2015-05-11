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

package com.intel.intelanalytics

import org.scalatest.WordSpec

class StringUtilsTest extends WordSpec {

  "StringUtils" should {

    "be true for alpha-numeric with underscores" in {
      assert(StringUtils.isAlphanumericUnderscore("a"))
      assert(StringUtils.isAlphanumericUnderscore("abc"))
      assert(StringUtils.isAlphanumericUnderscore("abc_def"))
      assert(StringUtils.isAlphanumericUnderscore("MixedCase"))
      assert(StringUtils.isAlphanumericUnderscore("Mixed_Case_With_Underscores"))
      assert(StringUtils.isAlphanumericUnderscore("_"))
      assert(StringUtils.isAlphanumericUnderscore("___"))
      assert(StringUtils.isAlphanumericUnderscore(""))
    }

    "be false for non- alpha-numeric with underscores" in {
      assert(!StringUtils.isAlphanumericUnderscore("per%cent"))
      assert(!StringUtils.isAlphanumericUnderscore("spa ce"))
      assert(!StringUtils.isAlphanumericUnderscore("doll$ar"))
      assert(!StringUtils.isAlphanumericUnderscore("pound#"))
      assert(!StringUtils.isAlphanumericUnderscore("period."))
      assert(!StringUtils.isAlphanumericUnderscore("explanation!"))
      assert(!StringUtils.isAlphanumericUnderscore("hy-phen"))
    }

  }

}
