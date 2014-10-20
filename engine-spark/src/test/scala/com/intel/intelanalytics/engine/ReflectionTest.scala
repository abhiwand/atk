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
import com.intel.intelanalytics.engine.plugin.{ Invocation, CommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import org.scalatest.{ Matchers, FlatSpec }
import scala.concurrent.ExecutionContext
import scala.reflect.runtime.{ universe => ru }
import ru._
import scala.tools.nsc.util.ScalaClassLoader.URLClassLoader
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

case class Mixed(frameId: Int, frame: FrameReference, graphId: Int, graph: GraphReference) {
}
case class Foo(bar: Int, quux: String)
object implicits {
  implicit val fmt = jsonFormat4(Mixed)

}

import implicits._

class MixedPlugin extends CommandPlugin[Mixed, Mixed] {
  override def name: String = ???

  override def execute(invocation: Invocation, arguments: Mixed)(implicit user: UserPrincipal, executionContext: ExecutionContext): Mixed = ???
}

class ReflectionTest extends FlatSpec with Matchers {

  //TODO: Re-enable before merge
  "getReferenceTypes" should "find frame references and graph references" in {

    val members = Reflection.getUriReferenceTypes[Mixed]().toArray

    members.length shouldBe 2
    members.map(_._1).toArray should be(Array("frame", "graph"))
  }

  it should "work with nested calls" in {
    def fun[T: TypeTag]() = {
      Reflection.getUriReferenceTypes[T]().toArray
    }
    val members = fun[Mixed]()

    members.length shouldBe 2
    members.map(_._1).toArray should be(Array("frame", "graph"))

  }

  //  it should "work with classes from other classloaders" in {
  //    val loader = new URLClassLoader(Array(getClass.getProtectionDomain.getCodeSource.getLocation), getClass.getClassLoader.getParent)
  //    val fooPlugin2 = loader.loadClass(classOf[MixedPlugin].getName)
  //    val inst = fooPlugin2.newInstance().asInstanceOf[CommandPlugin[_,_]]
  //    val arg = Mixed(1, FrameReference(2, None), 3, GraphReference(4))
  //    val argJson = new MixedPlugin().serializeArguments(arg)
  //    val newArg = inst.parseArguments(argJson)
  //    val members = Reflection.getUriReferenceTypes(newArg)
  //    members.length shouldBe 2
  //    members.map(_._1).toArray should be(Array("frame", "graph"))
  //  }

  "getConstructor" should "get case class constructors" in {

    val ctor = Reflection.getConstructor[Foo]

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

