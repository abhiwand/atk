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

import org.apache.commons.lang3.StringUtils

/**
 * Links with relations
 * @param rel the relationship of the link to the current document
 * @param uri the link
 * @param method the HTTP method that should be used to retrieve the link
 */
case class RelLink(rel: String, uri: String, method: String) {
  require(rel != null, "rel must not be null")
  require(uri != null, "uri must not be null")
  require(method != null, "method must not be null")
  require(Rel.AllowedMethods.contains(method), "method must be one of " + Rel.AllowedMethods.mkString(", "))
}

/**
 * Convenience methods for constructing RelLinks
 */
object Rel {

  val AllowedMethods = List("GET", "PUT", "POST", "HEAD", "DELETE", "OPTIONS")

  /**
   * Self links
   */
  def self(uri: String) = RelLink(rel = "self", uri = uri, method = "GET")
}
