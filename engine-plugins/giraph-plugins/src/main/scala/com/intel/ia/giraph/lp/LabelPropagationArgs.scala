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

package com.intel.ia.giraph.lp

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.frame.{ FrameEntity, FrameReference }
import com.intel.intelanalytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.apache.commons.lang3.StringUtils

/**
 * Arguments to the plugin - see user docs for more on the parameters
 */
case class LabelPropagationArgs(@ArgDoc("""""") frame: FrameReference,
                                @ArgDoc("""""") srcColName: String,
                                @ArgDoc("""""") destColName: String,
                                @ArgDoc("""""") weightColName: String,
                                @ArgDoc("""""") srcLabelColName: String,
                                @ArgDoc("""""") resultColName: Option[String] = None,
                                @ArgDoc("""""") maxIterations: Option[Int] = None,
                                @ArgDoc("""""") convergenceThreshold: Option[Float] = None,
                                @ArgDoc("""""") alpha: Option[Float] = None) {

  require(frame != null, "frame is required")
  require(StringUtils.isNotBlank(srcColName), "source column name property list is required")
  require(StringUtils.isNotBlank(destColName), "destination column name property list is required")
  require(srcColName != destColName, "source and destination column names cannot be the same")
  require(StringUtils.isNotBlank(weightColName), "edge weight property list is required")
  require(StringUtils.isNotBlank(srcLabelColName), "source label column name property list is required")

  def getResultsColName: String = {
    resultColName.getOrElse("resultLabels")
  }

  def getMaxIterations: Int = {
    val value = maxIterations.getOrElse(10)
    if (value < 1) 10 else value
  }

  def getConvergenceThreshold: Float = {
    convergenceThreshold.getOrElse(0.00000001f)
  }

  def getLambda: Float = {
    val value = alpha.getOrElse(0f)
    1 - Math.min(1, Math.max(0, value))
  }
}

case class LabelPropagationResult(outputFrame: FrameEntity, report: String) {
  require(outputFrame != null, "label results are required")
  require(StringUtils.isNotBlank(report), "report is required")
}

/** Json conversion for arguments and return value case classes */
object LabelPropagationJsonFormat {

  implicit val argsFormat = jsonFormat9(LabelPropagationArgs)
  implicit val resultFormat = jsonFormat2(LabelPropagationResult)
}
