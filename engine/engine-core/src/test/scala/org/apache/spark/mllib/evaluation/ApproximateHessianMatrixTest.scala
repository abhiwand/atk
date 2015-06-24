package org.apache.spark.mllib.evaluation

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.optimize.DiffFunction
import breeze.util.DoubleImplicits
import org.scalatest.{FlatSpec, Matchers}

class ApproximateHessianMatrixTest extends FlatSpec with Matchers with DoubleImplicits {

  val diffFunction = new DiffFunction[DenseVector[Double]] {
    def calculate(x: DenseVector[Double]) = {
      val value = 100 * Math.pow(x(1) - x(0) * x(0), 2) + Math.pow(1 - x(0), 2)
      val grad = DenseVector(-400 * x(0) * (x(1) - x(0) * x(0)) - 2 * (1 - x(0)),
        200 * (x(1) - x(0) * x(0)))
      (value, grad)
    }
  }

  "ApproximateHessian" should "compute hessian matrix using central difference" in {
    val x = DenseVector(-1.2d, 1d)
    val expectedHessian = DenseMatrix((1330d, 480d), (480d, 200d))
    val hessian = ApproximateHessianMatrix(diffFunction, x).calculate()

    assert(hessian.size === expectedHessian.size)
    for (i <- 0 until expectedHessian.rows; j <- 0 until expectedHessian.cols) {
      assert(hessian(i, j).closeTo(expectedHessian(i, j)))
    }
  }

  "ApproximateHessian" should "return an empty hessian matrix" in {
    val x = DenseVector[Double]()
    val hessian = ApproximateHessianMatrix(diffFunction, x).calculate()

    assert(hessian.size === 0)
  }

}
