package com.intel.graphbuilder.driver.spark.titan

import com.intel.graphbuilder.elements.Property
import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

class JoinBroadcastVariableTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {
  val personIds: List[(Property, AnyRef)] = List(
    (Property("name", "alice"), new java.lang.Long(1)),
    (Property("name", "bob"), new java.lang.Long(2)),
    (Property("name", "cathy"), new java.lang.Long(3)),
    (Property("name", "david"), new java.lang.Long(4)),
    (Property("name", "eva"), new java.lang.Long(5))
  )

  "JoinBroadcastVariable" should "create a single broadcast variable when RDD size is less than 2GB" in {
    val personRdd = sparkContext.parallelize(personIds)

    val broadcastVariable = JoinBroadcastVariable(personRdd)

    broadcastVariable.length() should equal(1)
    broadcastVariable.broadcastMaps(0).value.size should equal(5)
    personIds.map { case (property, id) => broadcastVariable.get(property) should equal(Some(id)) }
    broadcastVariable.get(Property("not defined", new java.lang.Long(1))).isDefined should equal(false)
  }
  "JoinBroadcastVariable" should "create an empty broadcast variable" in {
    val emptyList = sparkContext.parallelize(List.empty[(Property, AnyRef)])

    val broadcastVariable = JoinBroadcastVariable(emptyList)

    broadcastVariable.length() should equal(1)
    broadcastVariable.broadcastMaps(0).value.isEmpty should equal(true)
  }
  "JoinBroadcastVariable" should "throw an IllegalArgumentException if join parameter is null" in {
    intercept[IllegalArgumentException] {
      JoinBroadcastVariable(null)
    }
  }
}
