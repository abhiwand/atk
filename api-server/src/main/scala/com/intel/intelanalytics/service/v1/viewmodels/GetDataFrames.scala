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

package com.intel.intelanalytics.service.v1.viewmodels

/**
 * The REST service response for a single dataFrame in the list provided by "GET ../frames".
 *
 * @param id unique id auto-generated by the database
 * @param name name assigned by user
 * @param url the URI to 'this' dataFrame in terms of the REST API
 */
case class GetDataFrames(id: Long, name: Option[String], url: String, entityType: String) {
  require(id > 0, "id must be greater than zero")
  require(name != null, "name must not be null")
  require(url != null, "url must not be null")
  require(entityType != null, "entityType must not be null")
}
