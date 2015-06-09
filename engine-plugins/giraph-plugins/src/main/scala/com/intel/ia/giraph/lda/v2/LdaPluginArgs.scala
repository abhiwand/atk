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
