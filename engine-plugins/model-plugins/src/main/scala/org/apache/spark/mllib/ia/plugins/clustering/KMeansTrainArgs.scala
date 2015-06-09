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

package org.apache.spark.mllib.ia.plugins.clustering

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.model.ModelReference

/**
 * Command for loading model data into existing model in the model database.
 * @param model Handle to the model to be written to.
 * @param frame Handle to the data frame
 * @param observationColumns Handle to the observation column of the data frame
 * @param k Optional number of clusters. Default is 2
 * @param maxIterations Optional number of iterations to run the algorithm. Default is 20
 * @param epsilon Optional distance threshold within which we've considered centers to have converged. Default is 1e-4
 * @param initializationMode Optional initialization algorithm can be "random" or "k-means||". Default is "k-means||"
 */
case class KMeansTrainArgs(model: ModelReference,
                           frame: FrameReference,
                           observationColumns: List[String],
                           columnScalings: List[Double],
                           k: Option[Int] = None,
                           maxIterations: Option[Int] = None,
                           epsilon: Option[Double] = None,
                           initializationMode: Option[String] = None) {
  require(model != null, "model must not be null")
  require(frame != null, "frame must not be null")
  require(observationColumns != null && !observationColumns.isEmpty, "observationColumn must not be null nor empty")
  require(columnScalings != null && !columnScalings.isEmpty, "columnWeights must not be null or empty")
  require(columnScalings.length == observationColumns.length, "Length of columnWeights and observationColumns needs to be the same")

  def getK: Int = {
    if (k.isDefined) {
      require(k.get > 0, "k must be at least 1")
    }
    k.getOrElse(2)
  }

  def getMaxIterations: Int = {
    if (maxIterations.isDefined) {
      require(maxIterations.get > 0, "maxIterations must be a positive value")
    }
    maxIterations.getOrElse(20)
  }

  def geteEpsilon: Double = {
    if (epsilon.isDefined) {
      require(epsilon.get > 0.0, "epsilon must be a positive value")
    }
    epsilon.getOrElse(1e-4)
  }

  def getInitializationMode: String = {
    initializationMode.getOrElse("k-means||")
  }
}

/**
 * Return object when training a KMeansModel
 * @param clusterSize A dictionary containing the number of elements in each cluster
 * @param withinSetSumOfSquaredError  Within cluster sum of squared distance
 */
case class KMeansTrainReturn(clusterSize: Map[String, Int], withinSetSumOfSquaredError: Double)
