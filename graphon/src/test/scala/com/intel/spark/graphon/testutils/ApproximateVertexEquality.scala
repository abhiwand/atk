//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.spark.graphon.testutils

import com.intel.graphbuilder.elements.{ Property, GBVertex }
import com.intel.spark.graphon.VectorMath

/**
 * Provides methods for comparing vertices and sets of vertices when approximate equality is acceptable in a
 * list of specified properties. Properties that can be approximately equal must have values that are of one of the
 * types Float, Double, Vector[Double] or Vector[Float] valued
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

    v1.physicalId == v2.physicalId &&
      v1.gbId.equals(v2.gbId) &&
      keys1.equals(keys2) &&
      keys1.forall(k => (namesOfApproximateProperties.contains(k) &&
        propertiesApproximatelyEqual(v1.getProperty(k), v2.getProperty(k), threshold)) ||
        (v1.getProperty(k) equals v2.getProperty(k)))
  }

  /**
   * Test two sets of vertices for approximate equality.
   *
   * The equality check across sets is by brute force...  this is slower than hashing would be but
   * these methods are meant to be run on small sets when evaluating unit tests. Change it up if you have
   * a performance problem here.
   *
   * @param vertexSet1 First set of vertices.
   * @param vertexSet2 Second set of vertices.
   * @param namesOfApproximateProperties List of properties that are allowed to be approximately equal.
   * @param threshold Threshold for floating point comparisons when performing approximate comparisons of properties.
   * @return True if the two sets are the same size and for every vertex in the first set, there is a vertex in the
   *         second set with which it is approximately equal.
   */
  def approximatelyEquals(vertexSet1: Set[GBVertex],
                          vertexSet2: Set[GBVertex],
                          namesOfApproximateProperties: List[String],
                          threshold: Double): Boolean = {

    (vertexSet1.size == vertexSet2.size) &&
      vertexSet1.forall(v => vertexSet2.exists(u => approximatelyEquals(u, v, namesOfApproximateProperties, threshold)))
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
      (propertyOption1.get.value, propertyOption2.get.value) match {
        case (v1: Float, v2: Float) => Math.abs(v1 - v2) < threshold
        case (v1: Double, v2: Double) => Math.abs(v1 - v2) < threshold
        case (v1: Vector[_], v2: Vector[_]) => (v1.length == v2.length) &&
          (VectorMath.l1Distance(v1.asInstanceOf[Vector[Double]], v2.asInstanceOf[Vector[Double]]) < threshold)
        case _ => false
      }
    }
  }

}
