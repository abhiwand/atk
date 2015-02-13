//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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
    k.getOrElse(2)
  }

  def getMaxIterations: Int = {
    maxIterations.getOrElse(20)
  }

  def geteEpsilon: Double = {
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
