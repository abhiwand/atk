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

import breeze.linalg.{ DenseMatrix, DenseVector, diag }
import breeze.numerics.sqrt
import breeze.stats.distributions.ChiSquared
import com.intel.taproot.analytics.domain.frame.FrameEntity
import org.apache.spark.mllib.classification.LogisticRegressionModelWithFrequency

/**
 * Summary table with results of logistic regression train plugin
 *
 * @param numFeatures Number of features
 * @param numClasses Number of classes
 * @param coefficients Model coefficients
 *                     The dimension of the coefficients' vector is
 *                     (numClasses - 1) * (numFeatures + 1) if `addIntercept == true`, and
 *                     (numClasses - 1) * numFeatures if `addIntercept != true`
 * @param degreesFreedom Degrees of freedom for model coefficients
 * @param covarianceMatrix Optional covariance matrix
 * @param standardErrors Optional standard errors for model coefficients
 *                       The standard error for each variable is the square root of
 *                       the diagonal of the covariance matrix
 * @param waldStatistic Optional Wald Chi-Squared statistic
 *                      The Wald Chi-Squared statistic is the coefficients
 *                      divided by the standard errors
 * @param pValue Optional p-values for the model coefficients
 */
case class LogisticRegressionSummaryTable(numFeatures: Int,
                                          numClasses: Int,
                                          coefficients: Map[String, Double],
                                          degreesFreedom: Map[String, Double],
                                          covarianceMatrix: Option[FrameEntity] = None,
                                          standardErrors: Option[Map[String, Double]] = None,
                                          waldStatistic: Option[Map[String, Double]] = None,
                                          pValue: Option[Map[String, Double]] = None)

/**
 * Summary table builder
 *
 * @param logRegModel Logistic regression model
 * @param observationColumns Names of observation columns
 * @param isAddIntercept If true, intercept column was added to training data
 * @param hessianMatrix Optional Hessian matrix for trained model
 */
class SummaryTableBuilder(logRegModel: LogisticRegressionModelWithFrequency,
                          observationColumns: List[String],
                          isAddIntercept: Boolean,
                          hessianMatrix: Option[DenseMatrix[Double]]) {

  val interceptName = "intercept"
  val coefficients = computeCoefficients
  val coefficientNames = computeCoefficientNames
  val approxCovarianceMatrix = computeApproxCovarianceMatrix
  val degreesFreedom = computeDegreesFreedom

  /**
   * Build summary table for trained model
   */
  def build(covarianceFrame: Option[FrameEntity]): LogisticRegressionSummaryTable = {
    approxCovarianceMatrix match {
      case Some(approxMatrix) => {
        val stdErrors = computeStandardErrors(coefficients, approxMatrix.covarianceMatrix)
        val waldStatistic = computeWaldStatistic(coefficients, stdErrors)
        val pValues = computePValue(waldStatistic)

        LogisticRegressionSummaryTable(
          logRegModel.numFeatures,
          logRegModel.numClasses,
          (coefficientNames zip coefficients.toArray).toMap,
          (coefficientNames zip degreesFreedom.toArray).toMap,
          covarianceFrame,
          Some((coefficientNames zip stdErrors.toArray).toMap),
          Some((coefficientNames zip waldStatistic.toArray).toMap),
          Some((coefficientNames zip pValues.toArray).toMap)
        )
      }
      case _ => LogisticRegressionSummaryTable(
        logRegModel.numFeatures,
        logRegModel.numClasses,
        (coefficientNames zip coefficients.toArray).toMap,
        (coefficientNames zip degreesFreedom.toArray).toMap
      )
    }
  }

  /**
   *
   * @return
   */
  def computeApproxCovarianceMatrix: Option[ApproximateCovarianceMatrix] = {
    hessianMatrix match {
      case Some(hessian) => Some(ApproximateCovarianceMatrix(hessian, isAddIntercept))
      case _ => None
    }
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
  def computeCoefficientNames: List[String] = {
    val names = if (logRegModel.numClasses > 2) {
      for (i <- 0 until logRegModel.numFeatures) yield s"${observationColumns(i)}_${i + 1}"
    }
    else {
      observationColumns
    }

    if (isAddIntercept) {
      (interceptName +: names).toList
    }
    else {
      names.toList
    }
  }

  /**
   *
   * @return
   */
  def computeDegreesFreedom: DenseVector[Double] = {
    DenseVector.fill(coefficients.length) {
      1
    }
  }

  /**
   *
   * @param coefficients
   * @param covarianceMatrix
   * @return
   */
  def computeStandardErrors(coefficients: DenseVector[Double], covarianceMatrix: DenseMatrix[Double]): DenseVector[Double] = {
    sqrt(diag(covarianceMatrix))
  }

  /**
   *
   * @param coefficients
   * @param stdErrors
   * @return
   */
  def computeWaldStatistic(coefficients: DenseVector[Double], stdErrors: DenseVector[Double]): DenseVector[Double] = {
    coefficients :/ stdErrors //element-wise division
  }

  /**
   *
   * @param waldStatistic
   * @return
   */
  def computePValue(waldStatistic: DenseVector[Double]): DenseVector[Double] = {
    val chi = ChiSquared(1)
    waldStatistic.map(w => 1 - chi.cdf(w))
  }
}
