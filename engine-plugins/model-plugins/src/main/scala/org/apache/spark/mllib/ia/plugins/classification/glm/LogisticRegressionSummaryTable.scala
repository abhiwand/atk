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
package org.apache.spark.mllib.ia.plugins.classification.glm

import breeze.linalg.{DenseMatrix, DenseVector, diag}
import breeze.numerics.sqrt
import breeze.stats.distributions.ChiSquared
import com.intel.taproot.analytics.domain.CreateEntityArgs
import com.intel.taproot.analytics.domain.frame.{FrameMeta, FrameEntity}
import com.intel.taproot.analytics.engine.spark.frame.SparkFrameData
import org.apache.spark.mllib.classification.LogisticRegressionModelWithFrequency

/**
 * Results for logistic regression train plugin
 *
 * @param numFeatures Number of features
 * @param numClasses Number of classes
 * @param coefficients Model coefficients
 * @param covarianceMatrix Optional covariance matrix
 */
case class LogisticRegressionSummaryTable(numFeatures: Int,
                                          numClasses: Int,
                                          coefficients: Map[String, Double],
                                          covarianceMatrix: Option[FrameEntity] = None,
                                          standardErrors: Option[Map[String, Double]] = None,
                                          waldStatistic: Option[Map[String, Double]] = None,
                                          pValue: Option[Map[String, Double]] = None,
                                          degreesFreedom: Option[Map[String, Double]] = None) {

  def this(logRegModel: LogisticRegressionModelWithFrequency,
           observationColumns: List[String],
           covarianceMatrix: Option[FrameEntity]) {
    this(logRegModel.numFeatures,
      logRegModel.numClasses,
      LogisticRegressionSummaryTable1.getCoefficients(logRegModel, observationColumns),
      covarianceMatrix)
  }

}

class SummaryTableBuilder(logRegModel: LogisticRegressionModelWithFrequency,
                          observationColumns: List[String],
                          isAddIntercept: Boolean) {

  val interceptName = "_intercept"
  val coefficients = computeCoefficients
  val coefficientNames = computeCoefficientNames

  def build(): LogisticRegressionSummaryTable = {
    val covarianceMatrix = ApproximateCovarianceMatrix(model)

    val frame = tryNew(CreateEntityArgs(
      description = Some("covariance matrix created by LogisticRegression train operation"))) {
      newTrainFrame: FrameMeta =>
        save(new SparkFrameData(newTrainFrame.meta, frameRdd))
    }.meta
    val stdErrors = computeStandardErrors(coefficients, covMatrix)
    val waldStatistic = computeWaldStatistic(coefficients, stdErrors)
    val pValues = computePValue(waldStatistic)
  }


  /**
   * The dimension of coefficients vector is (numClasses - 1) * (numFeatures + 1) if `addIntercept == true`,
   * and if `addIntercept != true`, the dimension will be (numClasses - 1) * numFeatures
   *
   * @return
   */
  def computeCoefficients: DenseVector[Double] = {
    if (isAddIntercept) {
      DenseVector(logRegModel.intercept +: logRegModel.weights.toArray)
    }
    else {
      DenseVector(logRegModel.weights.toArray)
    }
  }

  /**
   * The dimension of coefficients vector is (numClasses - 1) * (numFeatures + 1) if `addIntercept == true`,
   * and if `addIntercept != true`, the dimension will be (numClasses - 1) * numFeatures
   *
   * @return
   */
  def computeCoefficientNames: Seq[String] = {
    val names = if (logRegModel.numClasses > 2) {
      for (i <- 0 until logRegModel.numFeatures) yield s"${observationColumns(i)}_${i+1}"
    }
    else {
      observationColumns
    }

    if (isAddIntercept) interceptName +: names  else names
  }

  def computeStandardErrors(coefficients: DenseVector[Double], covarianceMatrix: DenseMatrix[Double]): DenseVector[Double] = {
    sqrt(diag(covarianceMatrix))
  }

  def computeWaldStatistic(coefficients: DenseVector[Double], stdErrors: DenseVector[Double]): DenseVector[Double] = {
    coefficients :/ stdErrors //element-wise division
  }

  def computePValue(waldStatistic: DenseVector[Double]): DenseVector[Double] = {
    val chi = ChiSquared(1)
    waldStatistic.map(w => 1 - chi.cdf(w))
  }

  def computeDegreesFreedom(coefficients: DenseVector[Double]):  DenseVector[Double] =  {
    DenseVector.fill(coefficients.length){logRegModel.numClasses - 1}
  }
}


object LogisticRegressionSummaryTable1 {
  /**
   * Get map of model coefficients
   * @param logRegModel Logistic regression model
   * @param observationColumns Observation columns
   * @return Map with names and values of model coefficients
   */
  def getCoefficients(logRegModel: LogisticRegressionModelWithFrequency,
                      observationColumns: List[String]): Map[String, Double] = {
    val coefficients = logRegModel.intercept +: logRegModel.weights.toArray
    val coefficientNames = List("intercept") ++ observationColumns
    (coefficientNames zip coefficients).toMap
  }
}