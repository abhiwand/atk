package org.apache.spark.api.python

import org.scalatest.WordSpec

class EnginePythonAccumulatorParamTest extends WordSpec {

  val accum = new EnginePythonAccumulatorParam()

  "EnginePythonAccumulatorParam" should {
    "have a zero value" in {
      assert(accum.zero(new java.util.ArrayList()).size() == 0)
    }

    "have a zero value when given a value" in {
      val list = new java.util.ArrayList[Array[Byte]]()
      list.add(Array[Byte](0))
      assert(accum.zero(list).size() == 0)
    }

    "should be able to add in place" in {
      val accum2 = new EnginePythonAccumulatorParam()

      val list1 = new java.util.ArrayList[Array[Byte]]()
      list1.add(Array[Byte](0))

      val list2 = new java.util.ArrayList[Array[Byte]]()
      list2.add(Array[Byte](0))

      assert(accum2.addInPlace(list1, list2).size() == 2)
    }
  }
}
