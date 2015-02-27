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

/**
 *
 */
object StringUtils {

  /**
   * Check if the supplied string is alpha numeric with underscores (used for column names, etc)
   */
  def isAlphanumericUnderscore(str: String): Boolean = {
    for (c <- str.iterator) {
      // Not sure if this is great but it is probably faster than regex
      // http://stackoverflow.com/questions/12831719/fastest-way-to-check-a-string-is-alphanumeric-in-java
      if (c < 0x30 || (c >= 0x3a && c <= 0x40) || (c > 0x5a && c < 0x5f) || (c > 0x5f && c <= 0x60) || c > 0x7a) {
        return false
      }
    }
    true
  }
}
