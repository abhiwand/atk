package com.intel.spark.graphon.testutils

import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }
import com.intel.spark.graphon.VectorMath

/**
 * Provides methods for comparing vertices when approximate equality is acceptable in a specified property.
 * Such properties be either Float, Double, Vector[Double] or Vector[Float] valued
 */
object ApproximateVertexEquality {

  /**
   * Test two vertices for approximate equality.
   * @param v1 First vertex.
   * @param v2 Second vertex.
   * @param namesOfApproximateProperties List of properties that are allowed to be approximately equal.
   * @param threshold Threshold for floating point comparisons when performing approximate comparisons of properties.
   * @return True if the two vertices have the same ids, they are exactly equal off from the list of
   *         approximate properties, and they are approximately equal on the list of properties specified.
   */
  def approximatelyEquals(v1: GBVertex, v2: GBVertex, namesOfApproximateProperties: List[String], threshold: Double): Boolean = {

    val properties1 = v1.fullProperties
    val properties2 = v2.fullProperties

    val keys1 = properties1.map({ case p: Property => p.key })
    val keys2 = properties2.map({ case p: Property => p.key })

    (v1.physicalId == v2.physicalId) &&
      (v1.gbId equals v2.gbId) &&
      (keys1 equals keys2) &&
      (keys1.forall(k => ((namesOfApproximateProperties.contains(k) &&
        propertiesApproximatelyEqual(v1.getProperty(k), v2.getProperty(k), threshold)) ||
        (v1.getProperty(k) equals v2.getProperty(k)))))
  }

  /**
   * Test two sets of vertices for approximate equality.
   *
   * @param vertexSet1 First set of vertices.
   * @param vertexSet2 Second set of vertices.
   * @param propertyNames List of properties that are allowed to be approximately equal.
   * @param threshold Threshold for floating point comparisons when performing approximate comparisons of properties.
   * @return True if the two sets are the same size and for every vertex in the first set, there is a vertex in the
   *         second set with which it is approximately equal.
   */
  def equalsApproximateAtProperty(vertexSet1: Set[GBVertex],
                                  vertexSet2: Set[GBVertex],
                                  propertyNames: List[String],
                                  threshold: Double): Boolean = {

    (vertexSet1.size == vertexSet2.size) &&
      vertexSet1.forall(v => vertexSet2.exists(u => approximatelyEquals(u, v, propertyNames, threshold)))
  }

  /*
   * Tests if two property options are approximately equal given a threshold.
   *
   * If both options are empty, true is returned.
   * If one option is empty but the other is not, false is returned.
   * If the two property values are not of the same type, false is returned.
   * If the property values are not Float, Double, Vector[Float] or Vector[Double], the result is false.
   * Otherwise, the two values are considered equal if their l1 distance is below the threshold.
   *
   * @param propertyOption1 First option for a property.
   * @param propertyOption2 Second option for a property.
   * @param threshold Threshold of comparision; if |x - y| < threshold then x and y are considered approximately
   *                  equal.
   * @return Results of the approximate comparison test for the property options.
   */
  private def propertiesApproximatelyEqual(propertyOption1: Option[Property],
                                           propertyOption2: Option[Property],
                                           threshold: Double): Boolean = {

    if (propertyOption1.isEmpty != propertyOption2.isEmpty) {
      false
    }
    else if (propertyOption1.isEmpty && propertyOption2.isEmpty) {
      true
    }
    else {
      val val1 = propertyOption1.get.value
      val val2 = propertyOption2.get.value

      if (val1.isInstanceOf[Float] && val2.isInstanceOf[Float]) {
        Math.abs(val1.asInstanceOf[Float] - val2.asInstanceOf[Float]) < threshold
      }
      else if (val1.isInstanceOf[Double] && val2.isInstanceOf[Double]) {
        Math.abs(val1.asInstanceOf[Double] - val2.asInstanceOf[Double]) < threshold
      }
      else if (val1.isInstanceOf[Vector[Double]] && val2.isInstanceOf[Vector[Double]]) {
        val v1 = val1.asInstanceOf[Vector[Double]]
        val v2 = val2.asInstanceOf[Vector[Double]]
        (v1.length == v2.length) && (VectorMath.l1Distance(v1, v2) < threshold)
      }
      else if (val1.isInstanceOf[Vector[Float]] && val2.isInstanceOf[Vector[Float]]) {
        val v1 = val1.asInstanceOf[Vector[Float]]
        val v2 = val2.asInstanceOf[Vector[Float]]
        (v1.length == v2.length) && (VectorMath.l1Distance(v1.map(x => x.toDouble), v2.map(x => x.toDouble)) < threshold)
      }
      else {
        false
      }
    }
  }


}
