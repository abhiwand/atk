package org.apache.spark.mllib.evaluation

import breeze.linalg.{ DenseMatrix => BDM }

trait HessianMatrix {

  /**
   * Get the Hessian matrix at the solution (i.e., final iteration)
   */
  def getHessianMatrix: Option[BDM[Double]]
}
