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

import com.intel.graphbuilder.elements.GBVertex
import org.scalatest.FlatSpec

class PrimitiveConverterTest extends FlatSpec {

  "PrimitiveConverter" should "be able to convert ints" in {
    PrimitiveConverter.primitivesToObjects(classOf[Int]) == classOf[java.lang.Integer]
  }

  it should "be able to convert longs" in {
    PrimitiveConverter.primitivesToObjects(classOf[Long]) == classOf[java.lang.Long]
  }

  it should "be able to convert booleans" in {
    PrimitiveConverter.primitivesToObjects(classOf[Boolean]) == classOf[java.lang.Boolean]
  }

  it should "be able to convert chars" in {
    PrimitiveConverter.primitivesToObjects(classOf[Char]) == classOf[java.lang.Character]
  }

  it should "be able to convert floats" in {
    PrimitiveConverter.primitivesToObjects(classOf[Float]) == classOf[java.lang.Float]
  }

  it should "be able to convert doubles" in {
    PrimitiveConverter.primitivesToObjects(classOf[Double]) == classOf[java.lang.Double]
  }

  it should "be able to convert bytes" in {
    PrimitiveConverter.primitivesToObjects(classOf[Byte]) == classOf[java.lang.Byte]
  }

  it should "be able to convert shorts" in {
    PrimitiveConverter.primitivesToObjects(classOf[Short]) == classOf[java.lang.Short]
  }

  it should "NOT convert non-primitive types like Vertex" in {
    PrimitiveConverter.primitivesToObjects(classOf[GBVertex]) == classOf[GBVertex]
  }

  it should "NOT convert non-primitive types like List" in {
    PrimitiveConverter.primitivesToObjects(classOf[List[String]]) == classOf[List[String]]
  }
}
