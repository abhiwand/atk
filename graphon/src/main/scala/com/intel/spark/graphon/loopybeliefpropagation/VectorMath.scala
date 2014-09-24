package com.intel.spark.graphon.loopybeliefpropagation

/**
 * A library of routines for performing normed linear algebra with vectors represented as Vector[Double]
 *

 *
 */
object VectorMath {

  /**
   * Takes the natural logarithm of each component in the vector.
   * @param v Incoming vector.
   * @return Vector whose ith entry is the natural logarithm of the ith entry of v.
   */
  def componentwiseLog(v: Vector[Double]): Vector[Double] = {
    v.map(Math.log(_))
  }

  /**
   * Creates a new vector whose ith component is e raised to the the ith component of the input.
   *
   * If  a component of the vector is Double.NegativeInfinity, the value in that component in the new vector is 0d
   *
   * @param v The input vector
   * @return A new vector whose ith component is e raised to the the ith component of the input.
   */
  def recoverFromComponentwiseLog(v: Vector[Double]) = {
    v.map({ case x: Double => if (x.isNegInfinity) 0 else Math.exp(x) })
  }

  /**
   * Take the per-component sum of two vectors.
   * @param v1 First input vector.
   * @param v2 Second input vector.
   * @return The componentwise sum of the two vectors. If their lengths differ, the result is truncated to the length
   *         of the shorter.
   */
  def sum(v1: Vector[Double], v2: Vector[Double]): Vector[Double] = {
    v1.zip(v2).map({ case (x, y) => x + y })
  }

  /**
   * TODO - is this the right name for this thing???
   * @param v1
   * @param v2
   * @return
   */
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
