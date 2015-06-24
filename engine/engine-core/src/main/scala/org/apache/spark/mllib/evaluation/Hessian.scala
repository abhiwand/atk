package org.apache.spark.mllib.evaluation

import breeze.linalg.{DenseMatrix => BDM}

trait Hessian {

  /**
   * Get the approximate Hessian matrix at the solution (i.e., final iteration)
   */
  def getHessianMatrix : Option[BDM[Double]]
}
