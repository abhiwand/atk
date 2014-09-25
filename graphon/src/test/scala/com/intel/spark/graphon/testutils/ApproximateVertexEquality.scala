package com.intel.spark.graphon.testutils

import com.intel.graphbuilder.elements.{ Property, Vertex => GBVertex, Edge => GBEdge }
import com.intel.spark.graphon.VectorMath

object ApproximateVertexEquality {

  def propertyApproximatelyEqual(v1: GBVertex, v2: GBVertex, propertyName: String, threshold: Double): Boolean = {

    val propertyOption1 = v1.getProperty(propertyName)
    val propertyOption2 = v2.getProperty(propertyName)

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
      else {

        false
      }
    }
  }

  def equalOffProperty(v1: GBVertex, v2: GBVertex, propertyName: String): Boolean = {

    val properties1 = v1.fullProperties
    val properties2 = v2.fullProperties

    val keys1 = properties1.map({ case p: Property => p.key })
    val keys2 = properties2.map({ case p: Property => p.key })

    (keys1 equals keys2) && (keys1.forall(k => ((k == propertyName) || (v1.getProperty(k) equals v2.getProperty(k)))))
  }

  def equalsApproximateAtProperty(v1: GBVertex, v2: GBVertex, propertyName: String, threshold: Double) = {
    propertyApproximatelyEqual(v1, v2, propertyName, threshold) && equalOffProperty(v1, v2, propertyName)
  }

  def equalsApproximateAtProperty(vertexSet1: Set[GBVertex],
                                  vertexSet2: Set[GBVertex],
                                  propertyName: String,
                                  threshold: Double): Boolean = {

    (vertexSet1.size == vertexSet2.size) &&
      vertexSet1.forall(v => vertexSet2.exists(u => equalsApproximateAtProperty(u, v, propertyName, threshold)))
  }
}
