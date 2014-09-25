package com.intel.spark.graphon

/**
 * A library of routines for performing normed linear algebra with vectors represented as Vector[Double]
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
   * @param v The input vector.
   * @return A new vector whose ith component is e raised to the the ith component of the input.
   */
  def componentwiseExponentiation(v: Vector[Double]) = {
    v.map({ case x: Double => Math.exp(x) })
  }

  /**
   * Take the per-component sum of two vectors. If one vector is shorter than the other, the shorter is padded with 0s.
   * @param v1 First input vector.
   * @param v2 Second input vector.
   * @return The component-wise sum of the two vectors. If their lengths differ, the result is truncated to the length
   *         of the shorter.
   */
  def sum(v1: Vector[Double], v2: Vector[Double]): Vector[Double] = {
    val length1 = v1.length
    val length2 = v2.length

    val liftedV1 = if (length1 < length2) {
      v1 ++ (1 to (length2 - length1)).map(x => 0d)
    }
    else {
      v1
    }

    val liftedV2 = if (length2 < length1) {
      v2 ++ (1 to (length1 - length2)).map(x => 0d)
    }
    else {
      v2
    }
    liftedV1.zip(liftedV2).map({ case (x, y) => x + y })
  }

  /**
   * Takes the component-wise product of two vectors by first taking their component-wise logarithms, then summing the
   * vectors and then exponentiating.
   * @param v1 The first vector.
   * @param v2 The second vector.
   * @return component-wise product of the two vectors.
   */
  def overflowProtectedProduct(v1: Vector[Double], v2: Vector[Double]): Option[Vector[Double]] = {
    overflowProtectedProduct(List(v1, v2))
  }

  /**
   * Option containing the component-wise product of a list of vectors by first taking their component-wise logarithms,
   * then summing and then exponentiating. The length of the resulting vector is the length of the longest vector in the
   * list; shorter vectors are padded with zeroes.
   *
   * @param vectors A list of vectors whose product is to be taken.
   * @return Option containing the component-wise product of the list of vectors.
   */
  def overflowProtectedProduct(vectors: List[Vector[Double]]): Option[Vector[Double]] = {

    if (vectors.isEmpty) {
      None
    }
    else {
      Some(componentwiseExponentiation(vectors.map(componentwiseLog(_)).reduce(sum)))
    }
  }

  /**
   * @param v Input vector.
   * @return The l1 norm of the vector.
   */
  def l1Norm(v: Vector[Double]): Double = {
    v.map(x => Math.abs(x)).reduce(_ + _)
  }

  /**
   * @param v Input vector.
   * @return The input vector rescaled to have l1-norm equal to 1 - unless the input vector is the zero vector,
   *         in which case the zero vector is returned.
   */
  def l1Normalize(v: Vector[Double]): Vector[Double] = {
    val norm = l1Norm(v)
    if (norm > 0d) {
      v.map(x => x / norm)
    }
    else {
      v // only happens if v is the zero vector
    }
  }

  /**
   * Computes the l1-distance between two vectors. If one vector is shorter than the other,
   * the shorter is padded with 0s.
   *
   * @param v1 A vector.
   * @param v2 Another vector.
   * @return The l1-distance between the two vectors.
   */
  def l1Distance(v1: Vector[Double], v2: Vector[Double]): Double = {

    val length1 = v1.length
    val length2 = v2.length

    val liftedV1 = if (length1 < length2) {
      v1 ++ (1 to (length2 - length1)).map(x => 0d)
    }
    else {
      v1
    }

    val liftedV2 = if (length2 < length1) {
      v2 ++ (1 to (length1 - length2)).map(x => 0d)
    }
    else {
      v2
    }

    l1Norm(liftedV1.zip(liftedV2).map({ case (x, y) => x - y }))
  }
}
