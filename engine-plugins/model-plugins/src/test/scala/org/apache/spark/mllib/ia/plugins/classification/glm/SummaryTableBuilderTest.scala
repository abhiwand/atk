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

import breeze.linalg.{ DenseMatrix, inv }
import com.intel.taproot.analytics.domain.frame.FrameEntity
import org.apache.spark.mllib.classification.LogisticRegressionModelWithFrequency
import org.apache.spark.mllib.linalg.DenseVector
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FlatSpec, Matchers }

class SummaryTableBuilderTest extends FlatSpec with Matchers with MockitoSugar {

  val tolerance = 1e-6

  "SummaryTableBuilder" should "build summary table without an intercept column" in {
    val model = mock[LogisticRegressionModelWithFrequency]
    val obsColumns = List("col1", "col2", "col3")
    val weights = Array(1.5, 2.0d, 3.4d)

    when(model.numClasses).thenReturn(2)
    when(model.numFeatures).thenReturn(obsColumns.size)
    when(model.intercept).thenReturn(0d)
    when(model.weights).thenReturn(new DenseVector(weights))

    val summaryTableBuilder = SummaryTableBuilder(model, obsColumns, isAddIntercept = false)
    val interceptName = summaryTableBuilder.interceptName
    val summaryTable = summaryTableBuilder.build()

    summaryTable.numClasses shouldBe 2
    summaryTable.numFeatures shouldBe 3
    obsColumns.zipWithIndex.foreach { case (col, i) => summaryTable.coefficients(col) shouldBe weights(i) +- tolerance }
    obsColumns.zipWithIndex.foreach { case (col, i) => summaryTable.degreesFreedom(col) shouldBe 1d +- tolerance }

    summaryTable.coefficients.get(interceptName) shouldBe empty
    summaryTable.degreesFreedom.get(interceptName) shouldBe empty
    summaryTable.standardErrors shouldBe empty
    summaryTable.waldStatistic shouldBe empty
    summaryTable.pValue shouldBe empty
    summaryTable.covarianceMatrix shouldBe empty
  }

  "SummaryTableBuilder" should "build summary table with an intercept column" in {
    val hessianMatrix = DenseMatrix((1330d, 480d), (480d, 200d))

    val model = mock[LogisticRegressionModelWithFrequency]
    val obsColumns = List("col1", "col2", "col3")
    val intercept = 4.5d
    val weights = Array(1.5, 2.0d, 3.4d)

    when(model.numClasses).thenReturn(2)
    when(model.numFeatures).thenReturn(obsColumns.size)
    when(model.intercept).thenReturn(intercept)
    when(model.weights).thenReturn(new DenseVector(weights))

    val summaryTableBuilder = SummaryTableBuilder(model, obsColumns, isAddIntercept = true)
    val interceptName = summaryTableBuilder.interceptName
    val summaryTable = summaryTableBuilder.build()

    summaryTable.numClasses shouldBe 2
    summaryTable.numFeatures shouldBe 3
    obsColumns.zipWithIndex.foreach { case (col, i) => summaryTable.coefficients(col) shouldBe weights(i) +- tolerance }
    obsColumns.zipWithIndex.foreach { case (col, i) => summaryTable.degreesFreedom(col) shouldBe 1d +- tolerance }

    summaryTable.coefficients(interceptName) shouldBe intercept +- tolerance
    summaryTable.degreesFreedom(interceptName) shouldBe 1d +- tolerance
    summaryTable.standardErrors shouldBe empty
    summaryTable.waldStatistic shouldBe empty
    summaryTable.pValue shouldBe empty
    summaryTable.covarianceMatrix shouldBe empty
  }

  "SummaryTableBuilder" should "build summary table for multinomial logistic regression with intercept and covariance matrix" in {
    val covarianceFrame = mock[FrameEntity]

    val inputCovMatrix = DenseMatrix(
      (5d, 8d, -9d, 7d, 5d),
      (0d, 6d, 0d, 4d, 4d),
      (0d, 0d, 3d, 2d, 5d),
      (0d, 0d, 0d, 1d, -5d),
      (0d, 0d, 0d, 0d, 1d)
    )
    val hessianMatrix = inv(inputCovMatrix)

    //Reordered covariance matrix where the intercept is stored in the first row and first column
    //instead of in the last row and last column of the matrix
    val reorderedCovMatrix = DenseMatrix(
      (1d, 0d, 0d, 0d, 0d),
      (5d, 5d, 8d, -9d, 7d),
      (4d, 0d, 6d, 0d, 4d),
      (5d, 0d, 0d, 3d, 2d),
      (-5d, 0d, 0d, 0d, 1d)
    )

    //If the number of classes > 2, then each observation variable occurs (numClasses - 1) times in the weights.
    val numClasses = 3
    val obsColumns = List("col1", "col2")
    val intercept = 0.9d
    val weights = Array(1.0, 2.5, 5.0, 7.5)

    val model = mock[LogisticRegressionModelWithFrequency]
    when(model.numClasses).thenReturn(numClasses)
    when(model.numFeatures).thenReturn(obsColumns.size)
    when(model.intercept).thenReturn(intercept)
    when(model.weights).thenReturn(new DenseVector(weights))

    val summaryTableBuilder = SummaryTableBuilder(model, obsColumns, isAddIntercept = true, Some(hessianMatrix))
    val interceptName = summaryTableBuilder.interceptName
    val summaryTable = summaryTableBuilder.build(Some(covarianceFrame))

    summaryTable.numClasses shouldBe numClasses
    summaryTable.numFeatures shouldBe obsColumns.size

    val expectedCoefNames = List(interceptName, "col1_0", "col1_1", "col2_0", "col2_1")
    val expectedCoefs = intercept +: weights
    val expectedErrors = List(1.0, 2.236068, 2.44949, 1.732051, 1.0) //square root of diagonal of covariance matrix
    val expectedWaldStats = List(0.9, 0.4472136, 1.0206207, 2.8867513, 7.5) //coefs divided by standard error

    expectedCoefNames.zipWithIndex.foreach {
      case (col, i) => summaryTable.coefficients(col) shouldBe expectedCoefs(i) +- tolerance
    }
    expectedCoefNames.zipWithIndex.foreach {
      case (col, i) => summaryTable.degreesFreedom(col) shouldBe 1d +- tolerance
    }
    expectedCoefNames.zipWithIndex.foreach {
      case (col, i) => summaryTable.standardErrors.get(col) shouldBe expectedErrors(i) +- tolerance
    }
    expectedCoefNames.zipWithIndex.foreach {
      case (col, i) => summaryTable.waldStatistic.get(col) shouldBe expectedWaldStats(i) +- tolerance
    }

    summaryTable.pValue shouldBe defined
    summaryTable.covarianceMatrix shouldBe defined
  }

  "SummaryTableBuilder" should "throw an IllegalArgumentException if logistic regression model is null" in {
    intercept[IllegalArgumentException] {
      SummaryTableBuilder(null, List("a", "b"))
    }
  }

  "SummaryTableBuilder" should "throw an IllegalArgumentException if list of observation columns is null" in {
    intercept[IllegalArgumentException] {
      val model = mock[LogisticRegressionModelWithFrequency]
      SummaryTableBuilder(model, null)
    }
  }

  "SummaryTableBuilder" should "throw an IllegalArgumentException if list of observation columns is empty" in {
    intercept[IllegalArgumentException] {
      val model = mock[LogisticRegressionModelWithFrequency]
      SummaryTableBuilder(model, List.empty[String])
    }
  }
}
