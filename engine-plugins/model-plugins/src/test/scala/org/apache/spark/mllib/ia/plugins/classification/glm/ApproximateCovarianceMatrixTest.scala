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

import breeze.linalg.{DenseMatrix, inv}
import com.intel.taproot.testutils.MatcherUtils._
import com.intel.taproot.testutils.TestingSparkContextFlatSpec
import org.apache.spark.mllib.classification.LogisticRegressionModelWithFrequency
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithmWithFrequency
import org.mockito.Mockito._
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

class ApproximateCovarianceMatrixTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {

  val glmModel = mock[GeneralizedLinearAlgorithmWithFrequency[LogisticRegressionModelWithFrequency]]

  "ApproximateCovarianceMatrix" should "compute covariance matrix when intercept is not added to model" in {
    val hessianMatrix = DenseMatrix((1330d, 480d), (480d, 200d))

    val model = mock[LogisticRegressionModelWrapper]
    when(model.getHessianMatrix).thenReturn(Some(hessianMatrix))
    when(model.getModel).thenReturn(glmModel)
    when(model.getModel.isAddIntercept).thenReturn(false)

    val covarianceMatrix = ApproximateCovarianceMatrix(hessianMatrix).covarianceMatrix
    val expectedCovariance = DenseMatrix((0.005617978, -0.01348315), (-0.013483146, 0.03735955))

    covarianceMatrix should not be ('empty)
    covarianceMatrix.get should equalWithToleranceMatrix(expectedCovariance)
  }

  "ApproximateCovarianceMatrix" should "compute re-ordered covariance matrix when intercept is added to model" in {
    //Reorder the matrix so that the intercept is stored in the first row and first column
    // instead of in the last row and last column of the matrix
    val matrix = DenseMatrix(
      (1d, 8d, -9d, 7d, 5d),
      (0d, 1d, 0d, 4d, 4d),
      (0d, 0d, 1d, 2d, 5d),
      (0d, 0d, 0d, 1d, -5d),
      (0d, 0d, 0d, 0d, 1d)
    )
    val hessianMatrix = inv(matrix)

    val reorderedMatrix = DenseMatrix(
      (1d, 0d, 0d, 0d, 0d),
      (5d, 1d, 8d, -9d, 7d),
      (4d, 0d, 1d, 0d, 4d),
      (5d, 0d, 0d, 1d, 2d),
      (-5d, 0d, 0d, 0d, 1d)
    )

    val model = mock[LogisticRegressionModelWrapper]
    when(model.getHessianMatrix).thenReturn(Some(hessianMatrix))
    when(model.getModel).thenReturn(glmModel)
    when(model.getModel.isAddIntercept).thenReturn(true)

    val covarianceMatrix = ApproximateCovarianceMatrix(model).covarianceMatrix

    covarianceMatrix should not be ('empty)
    covarianceMatrix.get should equalWithToleranceMatrix(reorderedMatrix)
  }


  "ApproximateCovarianceMatrix" should "throw an IllegalArgument exception if Hessian matrix is not invertable" in {
    intercept[IllegalArgumentException] {
      val hessianMatrix = DenseMatrix((1d, 0d, 0d), (-2d, 0d, 0d), (4d, 6d, 1d))

      val model = mock[LogisticRegressionModelWrapper]
      when(model.getHessianMatrix).thenReturn(Some(hessianMatrix))
      when(model.getModel).thenReturn(glmModel)
      when(model.getModel.isAddIntercept).thenReturn(true)

      ApproximateCovarianceMatrix(model).covarianceMatrix
    }
  }

  "ApproximateCovarianceMatrix" should "return None if Hessian matrix is empty" in {
    val model = mock[LogisticRegressionModelWrapper]
    when(model.getModel).thenReturn(glmModel)
    when(model.getHessianMatrix).thenReturn(None)

    val covarianceMatrix = ApproximateCovarianceMatrix(model).covarianceMatrix
    covarianceMatrix should be('empty)
  }
}
