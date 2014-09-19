package com.intel.spark.graphon.loopybeliefpropagation

object VectorMath {

  def sum(v1: List[Double], v2: List[Double]): List[Double] = v1.zip(v2).map({ case (x, y) => x + y })

  def product(v1: List[Double], v2: List[Double]): List[Double] = v1.zip(v2).map({ case (x, y) => x * y })

  def componentwiseMaximum(v: List[Double]): Double = {
    if (v.isEmpty) {
      0
    }
    else {
      v.reduce(Math.max(_, _))
    }
  }
}
