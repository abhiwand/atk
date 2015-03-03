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

import com.intel.intelanalytics.domain.Error
import spray.json.JsObject

/**
 * The REST service response for single command in "GET ../query/id"
 *
 * @param id unique id auto-generated by the database
 * @param name the name of the query to be performed. This name is purely for descriptive purposes.
 * @param arguments the arguments to the function. They are pulled directly from the QueryString
 * @param error StackTrace and/or other error text if it exists
 * @param complete True if this query is completed
 * @param result result of the query
 * @param links relevant links
 */
case class GetQuery(id: Option[Long], name: String, arguments: Option[JsObject], error: Option[Error],
                    complete: Boolean, result: Option[GetQueryPage], links: List[RelLink], correlationId: String = "") {
  require(name != null, "name must not be null")
  require(arguments != null, "arguments may not be null")
  require(links != null, "links may not be null")
  require(error != null, "error must not be null")
}
