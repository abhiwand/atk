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

import breeze.linalg.{DenseMatrix => BDM, inv}
import com.intel.intelanalytics.domain.schema.{Column, DataTypes, FrameSchema}
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.{SparkContext, sql}

import scala.util.Try

/**
 * Covariance matrix generated from model's hessian matrix
 *
 * Covariance matrix is the inverse of the Hessian matrix. The Hessian matrix is
 * the second-order partial derivatives of the model's log-likelihood function.
 *
 * @param model Logistic regression model
 */
case class ApproximateCovarianceMatrix(model: IaLogisticRegressionModel) {

  /** Optional covariance matrix generated if flag for computing Hessian matrix was true */
  val covarianceMatrix = computeCovarianceMatrix()

  /**
   * Convert covariance matrix to Frame RDD with a single column of type vector
   *
   * @param sparkContext Spark context
   * @param columnName Column name
   * @return Optional frame RDD with a single column of type vector
   */
  def toFrameRdd(sparkContext: SparkContext, columnName: String): Option[FrameRdd] = {
    covarianceMatrix match {
      case Some(matrix) => {
        val schema = FrameSchema(List(Column(columnName, DataTypes.vector(matrix.cols))))
        val numCols = matrix.cols
        val rows: IndexedSeq[sql.Row] = for {
          i <- 0 until matrix.rows
          rowArray = matrix(i, ::).t.toArray
        } yield new GenericRow(Array[Any](DataTypes.toVector(numCols)(rowArray)))

        val rdd = sparkContext.parallelize(rows)
        Some(new FrameRdd(schema, rdd))
      }
      case _ => None
    }
  }

  /** Compute covariance matrix from model's hessian matrix */
  private def computeCovarianceMatrix(): Option[BDM[Double]] = {
    model.getHessianMatrix match {
      case Some(hessianMatrix) => {
        val covarianceMatrix = Try(inv(hessianMatrix)).getOrElse({
          throw new scala.IllegalArgumentException("Could not compute covariance matrix: Hessian matrix is not invertable")
        })
        Some(covarianceMatrix)
      }
      case _ => None
    }
  }
}
