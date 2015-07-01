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
package org.apache.spark.mllib.evaluation

import breeze.linalg.support.CanCopy
import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.math.VectorSpace
import breeze.optimize.DiffFunction
import com.intel.intelanalytics.domain.schema.{Column, DataTypes, FrameSchema}
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization.{CostFunction, Gradient, Updater}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.{SparkContext, sql}

/**
 * Calculate the approximate Hessian matrix using central difference.
 *
 * H_{i,j} = \lim_h -> 0 ((f'(x_{i} + h*e_{j}) - f'(x_{i} + h*e_{j}))/4*h
 * + (f'(x_{j} + h*e_{i}) - f'(x_{j} + h*e_{i}))/4*h)
 *
 * where e_{i} is the unit vector with 1 in the i^^th position and zeros elsewhere
 *
 * A pull request for computing the approximate hessian has been submitted to Scala Breeze.
 * This class can be deleted once this pull request is merged to Breeze, and available in Spark.
 * @see https://github.com/scalanlp/breeze/pull/413
 *
 * @param df differentiable function
 * @param x the point we compute the hessian for
 * @param epsilon a small value
 *
 * @return Approximate hessian matrix
 */
case class ApproximateHessianMatrix(df: DiffFunction[DenseVector[Double]],
                                    x: DenseVector[Double],
                                    epsilon: Double = 1E-5) {
  require(df != null, "Differentiable function should not be null")
  require(x != null, "Input vector x should not be null")

  /**
   * Calculate the approximate Hessian matrix using central difference.
   *
   * @return Approximate Hessian matrix
   */
  def calculate()(implicit vs: VectorSpace[DenseVector[Double], Double],
                  copy: CanCopy[DenseVector[Double]]): DenseMatrix[Double] = {
    import vs._
    val n = x.length
    val hessian = DenseMatrix.zeros[Double](n, n)

    // second order differential using central differences
    val x_copy = copy(x)
    for (i <- 0 until n) {
      x_copy(i) = x(i) + epsilon
      val df1 = df.gradientAt(x_copy)

      x_copy(i) = x(i) - epsilon
      val df2 = df.gradientAt(x_copy)

      val gradient = (df1 - df2) / (2 * epsilon)
      hessian(i, ::) := gradient.t

      x_copy(i) = x(i)
    }

    // symmetrize the hessian
    for (i <- 0 until n) {
      for (j <- 0 until i) {
        val tmp = (hessian(i, j) + hessian(j, i)) * 0.5
        hessian(i, j) = tmp
        hessian(j, i) = tmp
      }
    }

    hessian
  }
}

object ApproximateHessianMatrix {

  /**
   * Compute hessian matrix for RDD of label, and feature variables
   *
   * @param data RDD of the set of data examples, each of the form (label, [feature values])
   * @param weights
   * @param gradient Gradient object (used to compute the gradient of the loss function of
   *                 one single data example)
   * @param updater Updater function to actually perform a gradient step in a given direction
   * @param regParam Regularization parameter
   * @param numExamples Number of training examples in RDD
   * @param computeHessian If true, compute Hessian matrix at the solution (i.e., final iteration)
   * @return Hessian matrix, if computeHessian is true, otherwise None
   */
  def computeHessianMatrix(data: RDD[(Double, Vector)],
                           weights: Vector,
                           gradient: Gradient,
                           updater: Updater,
                           regParam: Double,
                           numExamples: Long,
                           computeHessian: Boolean = true): Option[DenseMatrix[Double]] = {
    if (computeHessian) {
      val costFun =
        new CostFunction(data, gradient, updater, regParam, numExamples)
      Some(ApproximateHessianMatrix(costFun, weights.toBreeze.toDenseVector).calculate())
    }
    else {
      None
    }
  }

}
