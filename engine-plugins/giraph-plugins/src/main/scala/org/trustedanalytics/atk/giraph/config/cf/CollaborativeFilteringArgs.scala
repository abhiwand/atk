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

package org.trustedanalytics.atk.giraph.config.cf

import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }
import org.apache.commons.lang3.StringUtils

/**
 * Arguments to the plugin - see user docs for more on the parameters
 */
case class CollaborativeFilteringArgs(frame: FrameReference,
                                      @ArgDoc("""<TBD>""") userColName: String,
                                      @ArgDoc("""<TBD>""") itemColName: String,
                                      @ArgDoc("""<TBD>""") ratingColName: String,
                                      @ArgDoc("""<TBD>""") evaluationFunction: Option[String],
                                      @ArgDoc("""<TBD>""") numFactors: Option[Int],
                                      @ArgDoc("""<TBD>""") maxIterations: Option[Int] = None,
                                      @ArgDoc("""<TBD>""") convergenceThreshold: Option[Double] = None,
                                      @ArgDoc("""<TBD>""") regularization: Option[Float] = None,
                                      @ArgDoc("""<TBD>""") biasOn: Option[Boolean] = None,
                                      @ArgDoc("""<TBD>""") minValue: Option[Float] = None,
                                      @ArgDoc("""<TBD>""") maxValue: Option[Float] = None,
                                      @ArgDoc("""<TBD>""") learningCurveInterval: Option[Int] = None,
                                      @ArgDoc("""<TBD>""") cgdIterations: Option[Int] = None) {

  require(frame != null, "frame is required")
  require(StringUtils.isNotBlank(userColName), "user column name property list is required")
  require(StringUtils.isNotBlank(itemColName), "item column name property list is required")
  require(StringUtils.isNotBlank(ratingColName), "rating column name property list is required")

  def getEvaluationFunction: String = {
    val value = evaluationFunction.getOrElse(CollaborativeFilteringConstants.alsAlgorithm)
    if (!CollaborativeFilteringConstants.alsAlgorithm.equalsIgnoreCase(value) &&
      !CollaborativeFilteringConstants.cgdAlgorithm.equalsIgnoreCase(value)) CollaborativeFilteringConstants.alsAlgorithm else value
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
    val value = regularization.getOrElse(0f)
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

object CollaborativeFilteringConstants {
  val alsAlgorithm = "als"
  val cgdAlgorithm = "cgd"
  val reportFilename = "cf-learning-report"
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
