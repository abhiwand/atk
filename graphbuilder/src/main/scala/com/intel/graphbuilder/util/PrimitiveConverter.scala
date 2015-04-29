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

package com.intel.graphbuilder.util

/**
 * Convert Primitives to their Object equivalents
 *
 * e.g. classOf[scala.Int] to classOf[java.lang.Integer]
 *      classOf[scala.Long] to classOf[java.lang.Long]
 *      classOf[scala.Char] to classOf[java.lang.Char]
 *      etc.
 *
 * Titan doesn't support primitive properties so we convert them to their Object equivalents.
 *
 * Spark also has trouble de-serializing classOf[Int] because of the underlying Java classes it uses.
 */
object PrimitiveConverter {

  // Stable values are needed for pattern matching primitive classes
  // See http://stackoverflow.com/questions/7157143/how-can-i-match-classes-in-a-scala-match-statement
  private val int = classOf[Int]
  private val long = classOf[Long]
  private val float = classOf[Float]
  private val double = classOf[Double]
  private val byte = classOf[Byte]
  private val short = classOf[Short]
  private val boolean = classOf[Boolean]
  private val char = classOf[Char]

  /**
   * Convert Primitives to their Object equivalents
   *
   * e.g. classOf[scala.Int] to classOf[java.lang.Integer]
   *      classOf[scala.Long] to classOf[java.lang.Long]
   *      classOf[scala.Char] to classOf[java.lang.Char]
   *      etc.
   *
   * Titan doesn't support primitive properties so we convert them to their Object equivalents.
   *
   * Spark also has trouble de-serializing classOf[Int] because of the underlying Java classes it uses.
   *
   * @param dataType convert primitives to Objects, e.g. classOf[Int] to classOf[java.lang.Integer].
   * @return the dataType unchanged, unless it was a primitive
   */
  def primitivesToObjects(dataType: Class[_]): Class[_] = dataType match {
    case `int` => classOf[java.lang.Integer]
    case `long` => classOf[java.lang.Long]
    case `float` => classOf[java.lang.Float]
    case `double` => classOf[java.lang.Double]
    case `byte` => classOf[java.lang.Byte]
    case `short` => classOf[java.lang.Short]
    case `boolean` => classOf[java.lang.Boolean]
    case `char` => classOf[java.lang.Character]
    case default => default
  }

}
