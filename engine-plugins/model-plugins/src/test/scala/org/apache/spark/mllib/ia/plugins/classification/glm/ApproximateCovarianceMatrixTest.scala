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

import breeze.linalg.DenseMatrix
import com.intel.taproot.testutils.TestingSparkContextFlatSpec
import org.mockito.Mockito._
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar


class ApproximateCovarianceMatrixTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {

  "ApproximateCovarianceMatrix" should "compute covariance matrix using model's Hessian matrix" in {
    val hessianMatrix = DenseMatrix((1330d, 480d), (480d, 200d))
    val model = mock[IaLogisticRegressionModel]
    when(model.getHessianMatrix).thenReturn(Some(hessianMatrix))

    val covarianceMatrix = ApproximateCovarianceMatrix(model)
    val expectedCovariance = DenseMatrix((0.005617978, -0.1348315), (-0.013483146, 0.03735955))
    println(covarianceMatrix)

  }

}
