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

package com.intel.ia.giraph.lbp

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.frame.{ FrameEntity, FrameReference }
import org.apache.commons.lang3.StringUtils

/**
 * Arguments to the plugin - see user docs for more on the parameters
 */
case class LoopyBeliefPropagationArgs(frame: FrameReference,
                                      srcColName: String,
                                      destColName: String,
                                      weightColName: String,
                                      srcLabelColName: String,
                                      resultColName: Option[String] = None,
                                      ignoreVertexType: Option[Boolean] = None,
                                      maxIterations: Option[Int] = None,
                                      convergenceThreshold: Option[Float] = None,
                                      anchorThreshold: Option[Double] = None,
                                      smoothing: Option[Float] = None,
                                      maxProduct: Option[Boolean] = None,
                                      power: Option[Float] = None) {

  require(frame != null, "frame is required")
  require(StringUtils.isNotBlank(srcColName), "source column name property list is required")
  require(StringUtils.isNotBlank(destColName), "destination column name property list is required")
  require(srcColName != destColName, "source and destination column names cannot be the same")
  require(StringUtils.isNotBlank(weightColName), "edge weight property list is required")
  require(StringUtils.isNotBlank(srcLabelColName), "source label column name property list is required")

  def getResultsColName: String = {
    resultColName.getOrElse("resultLabels")
  }

  def getIgnoreVertexType: Boolean = {
    ignoreVertexType.getOrElse(true)
  }

  def getMaxIterations: Int = {
    val value = maxIterations.getOrElse(10)
    if (value < 1) 10 else value
  }

  def getConvergenceThreshold: Float = {
    convergenceThreshold.getOrElse(0.00000001f)
  }

  def getAnchorThreshold: Double = {
    val value = anchorThreshold.getOrElse(1d)
    if (value < 0d) 1d else value
  }

  def getSmoothing: Float = {
    val value = smoothing.getOrElse(2f)
    if (value < 0f) 2f else value
  }

  def getMaxProduct: Boolean = {
    maxProduct.getOrElse(false)
  }

  def getPower: Float = {
    val value = power.getOrElse(0f)
    if (value < 0f) 0f else value
  }
}

case class LoopyBeliefPropagationResult(outputFrame: FrameEntity, report: String) {
  require(outputFrame != null, "label results are required")
  require(StringUtils.isNotBlank(report), "report is required")
}

/** Json conversion for arguments and return value case classes */
object LoopyBeliefPropagationJsonFormat {

  implicit val argsFormat = jsonFormat13(LoopyBeliefPropagationArgs)
  implicit val resultFormat = jsonFormat2(LoopyBeliefPropagationResult)
}
