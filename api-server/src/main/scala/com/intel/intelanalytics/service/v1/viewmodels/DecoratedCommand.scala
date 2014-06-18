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

import com.intel.intelanalytics.domain.Error
import spray.json.JsObject

/**
 * Decorated command object
 * @param id command id
 * @param name The name of the command
 * @param arguments The arguments to the function.
 * @param error StackTrace and/or other error text if it exists
 * @param progress List of progress for all jobs initiated by this command
 * @param complete True if this command is completed
 * @param result result data for executing the command
 * @param links The link representing the command
 */
case class DecoratedCommand(id: Long, name: String, arguments: Option[JsObject], error: Option[Error], progress: List[Int],
                            complete: Boolean, result: Option[JsObject], links: List[RelLink]) {
  require(id > 0)
  require(name != null)
  require(arguments != null)
  require(links != null)
  require(error != null)
}
