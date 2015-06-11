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

package org.apache.spark.mllib.ia.plugins.classification

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.model.ModelReference

/**
 * Command for loading model data into existing model in the model database.
 * @param model Handle to the model to be written to.
 * @param frame Handle to the data frame
 * @param observationColumns Handle to the observation column of the data frame
 * @param numIterations Optional number of iterations to run the algorithm.
 * @param stepSize Optional number of stepSize.
 * @param regType L1 or L2 regularization
 * @param regParam Optional regularization parameter
 * @param miniBatchFraction Optional mini
 */
case class ClassificationWithSGDTrainArgs(model: ModelReference,
                                          frame: FrameReference,
                                          labelColumn: String,
                                          observationColumns: List[String],
                                          intercept: Option[Boolean] = None,
                                          numIterations: Option[Int] = None,
                                          stepSize: Option[Int] = None,
                                          regType: Option[String] = None,
                                          regParam: Option[Double] = None,
                                          miniBatchFraction: Option[Double] = None) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && !observationColumns.isEmpty, "observationColumn must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")

  def getNumIterations: Int = {
    if (numIterations.isDefined) { require(numIterations.get > 0, "numIterations must be a positive value") }
    numIterations.getOrElse(100)
  }

  def getIntercept: Boolean = { intercept.getOrElse(true) }

  def getStepSize: Int = { stepSize.getOrElse(1) }

  def getRegParam: Double = { regParam.getOrElse(0.01) }

  def getMiniBatchFraction: Double = { miniBatchFraction.getOrElse(1.0) }

}
