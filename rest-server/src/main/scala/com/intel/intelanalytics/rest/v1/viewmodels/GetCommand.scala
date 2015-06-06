/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.rest.v1.viewmodels

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
 * @param complete True if this command is completed
 * @param result result data for executing the command
 * @param links hyperlinks to related URIs
 */
case class GetCommand(id: Long,
                      name: String,
                      correlationId: String,
                      arguments: Option[JsObject],
                      error: Option[Error],
                      progress: List[ProgressInfo],
                      complete: Boolean,
                      result: Option[JsObject],
                      links: List[RelLink]) {
  require(id > 0, "id must be greater than zero")
  require(name != null, "name may not be null")
  require(correlationId != null, "correlationId may not be null")
  require(arguments != null, "arguments may not be null")
  require(links != null, "links may not be null")
  require(error != null, "links may not be null")
}
