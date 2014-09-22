package com.intel.spark.graphon.loopybeliefpropagation

object VectorMath {

  def sum(v1: Vector[Double], v2: Vector[Double]): Vector[Double] = {
    v1.zip(v2).map({ case (x, y) => x + y })
  }

  def product(v1: Vector[Double], v2: Vector[Double]): Vector[Double] = v1.zip(v2).map({ case (x, y) => x * y })

  def product(vectors: List[Vector[Double]]): Vector[Double] = {
    vectors.reduce(product)
  }

  def componentwiseMaximum(v: Vector[Double]): Double = {
    if (v.isEmpty) {
      0
    }
    else {
      v.reduce(Math.max(_, _))
    }
  }

  def l1Norm(v: Vector[Double]): Double = {
    v.map(x => Math.abs(x)).reduce(_ + _)
  }

  def l1Normalize(v: Vector[Double]): Vector[Double] = {
    val norm = l1Norm(v)
    if (norm > 0d) {
      v.map(x => x / norm)
    }
    else {
      v // only happens if it's the zero vector
    }
  }
}
