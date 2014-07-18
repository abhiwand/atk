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
import com.intel.intelanalytics.engine.ProgressInfo

/**
 * The REST service response for single command in "GET ../commands/id"
 *
 * @param id unique id auto-generated by the database
 * @param name the name of the command to be performed. In the case of a builtin command, this name is used to
 *             find the stored implementation. For a user command, this name is purely for descriptive purposes.
 * @param arguments the arguments to the function. In some cases, such as line parsers, the arguments are configuration
 *                  arguments that configure the parser before any input arrives. In other cases, such as training an
 *                  ML algorithm, the parameters are used to execute the function directly.
 * @param error StackTrace and/or other error text if it exists
 * @param progress List of progress for each job initiated by the command
 * @param progressMessage List of detailed progress message for each job initiated by the command
 * @param complete True if this command is completed
 * @param result result data for executing the command
 * @param links hyperlinks to related URIs
 */
case class GetCommand(id: Long, name: String, arguments: Option[JsObject], error: Option[Error], progress: List[Float],
                      progressMessage: List[String], complete: Boolean, result: Option[JsObject], links: List[RelLink]) {
  require(id > 0, "id must be greater than zero")
  require(name != null, "name may not be null")
  require(arguments != null, "arguments may not be null")
  require(links != null, "links may not be null")
  require(error != null, "links may not be null")
}
