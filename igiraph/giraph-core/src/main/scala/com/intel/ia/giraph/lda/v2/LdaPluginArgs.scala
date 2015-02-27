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

package com.intel.ia.giraph.lda.v2

import com.intel.intelanalytics.domain.frame.{ FrameEntity, FrameReference }
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.domain.model.ModelReference
import org.apache.commons.lang3.StringUtils

/**
 * Arguments to the LDA plugin - see user docs for more on the parameters
 */
case class LdaTrainArgs(model: ModelReference,
                        frame: FrameReference,
                        documentColumnName: String,
                        wordColumnName: String,
                        wordCountColumnName: String,
                        maxIterations: Option[Int] = None,
                        alpha: Option[Float] = None,
                        beta: Option[Float] = None,
                        convergenceThreshold: Option[Float] = None,
                        evaluateCost: Option[Boolean] = None,
                        numTopics: Option[Int] = None) {

  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(StringUtils.isNotBlank(documentColumnName), "document column name is required")
  require(StringUtils.isNotBlank(wordColumnName), "word column name is required")
  require(StringUtils.isNotBlank(wordCountColumnName), "word count column name is required")
  require(maxIterations.isEmpty || maxIterations.get > 0, "Max iterations should be greater than 0")
  require(alpha.isEmpty || alpha.get > 0, "Alpha should be greater than 0")
  require(beta.isEmpty || beta.get > 0, "Beta should be greater than 0")
  require(convergenceThreshold.isEmpty || convergenceThreshold.get >= 0, "Convergence threshold should be greater than or equal to 0")
  require(numTopics.isEmpty || numTopics.get > 0, "Number of topics (K) should be greater than 0")

  def columnNames: List[String] = {
    List(documentColumnName, wordColumnName, wordCountColumnName)
  }

  def getMaxIterations: Int = {
    maxIterations.getOrElse(20)
  }

  def getAlpha: Float = {
    alpha.getOrElse(0.1f)
  }

  def getBeta: Float = {
    beta.getOrElse(0.1f)
  }

  def getConvergenceThreshold: Float = {
    convergenceThreshold.getOrElse(0.001f)
  }

  def getEvaluateCost: Boolean = {
    evaluateCost.getOrElse(false)
  }

  def getNumTopics: Int = {
    numTopics.getOrElse(10)
  }

}

case class LdaTrainResult(docResults: FrameEntity, wordResults: FrameEntity, report: String) {
  require(docResults != null, "document results are required")
  require(wordResults != null, "word results are required")
  require(StringUtils.isNotBlank(report), "report is required")
}

/** Json conversion for arguments and return value case classes */
object LdaJsonFormat {
  import com.intel.intelanalytics.domain.DomainJsonProtocol._
  implicit val ldaFormat = jsonFormat11(LdaTrainArgs)
  implicit val ldaResultFormat = jsonFormat3(LdaTrainResult)
}
