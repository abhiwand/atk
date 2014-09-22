package com.intel.spark.graphon.loopybeliefpropagation

object VectorMath {

  def sum(v1: List[Double], v2: List[Double]): List[Double] = {
    v1.zip(v2).map({ case (x, y) => x + y })
  }

  def product(v1: List[Double], v2: List[Double]): List[Double] = v1.zip(v2).map({ case (x, y) => x * y })

  def product(vectors: List[List[Double]]) : List[Double] = {
    vectors.reduce(product)
  }

  def componentwiseMaximum(v: List[Double]): Double = {
    if (v.isEmpty) {
      0
    }
    else {
      v.reduce(Math.max(_, _))
    }
  }

  def l1Norm(v : List[Double]) : Double = {
    v.map(x => Math.abs(x)).reduce(_+_)
  }

  def l1Normalize(v: List[Double]) : List[Double] = {
    val norm = l1Norm(v)
    if (norm > 0d) {
      v.map(x => x / norm)
    } else {
      v // only happens it it's the zero vector
    }
  }
}
