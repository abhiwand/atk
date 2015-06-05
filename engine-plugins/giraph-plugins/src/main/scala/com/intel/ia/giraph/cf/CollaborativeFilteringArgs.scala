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