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

package com.intel.ia.giraph.cf

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.frame.{ FrameEntity, FrameReference }
import org.apache.commons.lang3.StringUtils

/**
 * Arguments to the plugin - see user docs for more on the parameters
 */
case class CollaborativeFilteringArgs(frame: FrameReference,
                                      userColName: String,
                                      itemColName: String,
                                      ratingColName: String,
                                      evaluationFunction: Option[String],
                                      numFactors: Option[Int],
                                      maxIterations: Option[Int] = None,
                                      convergenceThreshold: Option[Double] = None,
                                      lambda: Option[Float] = None,
                                      biasOn: Option[Boolean] = None,
                                      maxValue: Option[Float] = None,
                                      minValue: Option[Float] = None,
                                      learningCurveInterval: Option[Int] = None,
                                      cgdIterations: Option[Int] = None) {
  val alsAlgorithm = "als"
  val cgdAlgorithm = "cgd"

  require(frame != null, "frame is required")
  require(StringUtils.isNotBlank(userColName), "user column name property list is required")
  require(StringUtils.isNotBlank(itemColName), "item column name property list is required")
  require(StringUtils.isNotBlank(ratingColName), "rating column name property list is required")

  def getEvaluationFunction: String = {
    val value = evaluationFunction.getOrElse(alsAlgorithm)
    if (!alsAlgorithm.equalsIgnoreCase(value) && !cgdAlgorithm.equalsIgnoreCase(value)) alsAlgorithm else value
  }

  def getNumFactors: Int = {
    val value = numFactors.getOrElse(3)
    if (value < 1) 3 else value
  }

  def getMaxIterations: Int = {
    val value = maxIterations.getOrElse(10)
    if (value < 1) 10 else value
  }

  def getConvergenceThreshold: Double = {
    convergenceThreshold.getOrElse(0.00000001f)
  }

  def getLambda: Float = {
    val value = lambda.getOrElse(0f)
    if (value < 0f) 0f else value
  }

  def getBias: Boolean = {
    biasOn.getOrElse(true)
  }

  def getMaxValue: Float = {
    val value = maxValue.getOrElse(10f)
    if (value < 1) 10f else value
  }

  def getMinValue: Float = {
    val value = minValue.getOrElse(0f)
    if (value < 0) 0f else value
  }

  def getLearningCurveInterval: Int = {
    val value = learningCurveInterval.getOrElse(1)
    if (value < 1) 1 else value
  }

  def getCgdIterations: Int = {
    val value = cgdIterations.getOrElse(2)
    if (value < 2) 2 else value
  }
}

case class CollaborativeFilteringResult(userFrame: FrameEntity, itemFrame: FrameEntity, report: String) {
  require(userFrame != null, "user frame is required")
  require(itemFrame != null, "item frame is required")
  require(StringUtils.isNotBlank(report), "report is required")
}

/** Json conversion for arguments and return value case classes */
object CollaborativeFilteringJsonFormat {

  implicit val argsFormat = jsonFormat14(CollaborativeFilteringArgs)
  implicit val resultFormat = jsonFormat3(CollaborativeFilteringResult)
}
