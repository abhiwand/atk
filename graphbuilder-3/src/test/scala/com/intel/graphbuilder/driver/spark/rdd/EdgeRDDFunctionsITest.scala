package com.intel.graphbuilder.driver.spark.rdd

import org.specs2.mutable.Specification
import com.intel.graphbuilder.testutils.TestingSparkContext
import com.intel.graphbuilder.elements.{GbIdToPhysicalId, Property, Edge}
import GraphBuilderRDDImplicits._

class EdgeRDDFunctionsITest extends Specification {

  "EdgeRDDFunctions" should {

    // A lot of tests are being grouped together here because it
    // is somewhat expensive to spin up a testing SparkContext
    "pass integration test" in new TestingSparkContext {

      val edge1 = new Edge(Property("gbId", 1L), Property("gbId", 2L), "myLabel", List(Property("key", "value")))
      val edge2 = new Edge(Property("gbId", 2L), Property("gbId", 3L), "myLabel", List(Property("key", "value")))
      val edge3 = new Edge(Property("gbId", 1L), Property("gbId", 2L), "myLabel", List(Property("key2", "value2")))

      val gbIdToPhysicalId1 = new GbIdToPhysicalId(Property("gbId", 1L), new java.lang.Long(1001L))
      val gbIdToPhysicalId2 = new GbIdToPhysicalId(Property("gbId", 2L), new java.lang.Long(1002L))

      val edges = sc.parallelize(List(edge1, edge2, edge3))
      val gbIdToPhysicalIds = sc.parallelize(List(gbIdToPhysicalId1, gbIdToPhysicalId2))

      val merged = edges.mergeDuplicates()
      merged.count() mustEqual 2

      val biDirectional = edges.biDirectional()
      biDirectional.count() mustEqual 6

      val mergedBiDirectional = biDirectional.mergeDuplicates()
      mergedBiDirectional.count() mustEqual 4

      val filtered = biDirectional.filterEdgesWithoutPhysicalIds()
      filtered.count() mustEqual 0

      val joined = biDirectional.joinWithPhysicalIds(gbIdToPhysicalIds)
      joined.count() mustEqual 4
      joined.filterEdgesWithoutPhysicalIds().count() mustEqual 4

      val vertices = edges.verticesFromEdges()
      vertices.count() mustEqual 6
      vertices.mergeDuplicates().count() mustEqual 3

    }
  }

}

