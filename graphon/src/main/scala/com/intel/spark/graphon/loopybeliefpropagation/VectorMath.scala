package com.intel.spark.graphon.loopybeliefpropagation

object VectorMath {

  def componentwiseLog(v: Vector[Double]): Vector[Double] = {
    v.map(Math.log(_))
  }

  def recoverFromComponentwiseLog(v: Vector[Double]) = {
    v.map({ case x: Double => if (x.isNegInfinity) 0 else Math.exp(x) })
  }

  def sum(v1: Vector[Double], v2: Vector[Double]): Vector[Double] = {
    v1.zip(v2).map({ case (x, y) => x + y })
  }

  def overflowProtectedProduct(v1: Vector[Double], v2: Vector[Double]): Vector[Double] = {
    overflowProtectedProduct(List(v1, v2))
  }

  def overflowProtectedProduct(vectors: List[Vector[Double]]): Vector[Double] = {

    if (vectors.isEmpty) {
      Vector(0.0d) // should we take the length as an argument?
    }
    else {
      recoverFromComponentwiseLog(vectors.map(componentwiseLog(_)).reduce(sum))
    }

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

  def l1Distance(v1: Vector[Double], v2: Vector[Double]): Double = {
    l1Norm(v1.zip(v2).map({ case (x, y) => x - y }))
  }
}
