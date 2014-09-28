//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.graph.GraphReference
import org.scalatest.{ Matchers, FlatSpec }

case class Mixed(frameId: Int, frame: FrameReference, graphId: Int, graph: GraphReference)

class ReflectionTest extends FlatSpec with Matchers {

  "getReferenceTypes" should "find frame references and graph references" in {

    val members = Reflection.getUriReferenceTypes[Mixed]().toArray

    members.length shouldBe 2
    members.map(_._1).toArray shouldBe Array("frame", "graph")
  }

  "getConstructor" should "get case class constructors" in {

    val ctor = Reflection.getConstructor[Mixed]()

    val foo = ctor(Seq(3, "hello"))

    foo shouldBe Foo(3, "hello")

  }

  it should " not work with inner classes" in {

    intercept[ScalaReflectionException] {
      val ctor = Reflection.getConstructor[Baz]()
    }

  }

  "getConstructorMap" should "work with case class constructors" in {

    val ctor = Reflection.getConstructorMap[Foo]()

    val foo = ctor(Map("bar" -> 3, "quux" -> "hello"))

    foo shouldBe Foo(3, "hello")

  }

  it should "throw IllegalArgumentException when parameters are not specified" in {

    val ctor = Reflection.getConstructorMap[Foo]()

    intercept[IllegalArgumentException] {
      val foo = ctor(Map("foo" -> 3, "quux" -> "hello"))
    }
  }

  case class Baz(foo: Int, quux: String)

}

case class Foo(bar: Int, quux: String)

