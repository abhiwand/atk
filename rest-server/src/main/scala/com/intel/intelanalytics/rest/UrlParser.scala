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

package com.intel.intelanalytics.rest

/**
 * Utility method for getting ids out of URL's
 */
object UrlParser {

  private val frameIdRegex = "/frames/(\\d+)".r

  /**
   * Get the frameId out of a URL in the format "../frames/id"
   * @return unique id
   */
  def getFrameId(url: String): Option[Long] = {
    val id = frameIdRegex.findFirstMatchIn(url).map(m => m.group(1))
    id.map(s => s.toLong)
  }

  private val graphIdRegex = "/graphs/(\\d+)".r

  /**
   * Get the graphId out of a URL in the format "../graphs/id"
   * @return unique id
   */
  def getGraphId(url: String): Option[Long] = {
    val id = graphIdRegex.findFirstMatchIn(url).map(m => m.group(1))
    id.map(s => s.toLong)
  }

}
