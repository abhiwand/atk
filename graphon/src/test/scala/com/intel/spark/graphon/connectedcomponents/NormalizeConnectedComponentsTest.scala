package com.intel.spark.graphon.connectedcomponents

import org.scalatest.Matchers

import java.util.Date
import org.apache.spark.SparkContext
import scala.concurrent.Lock
import org.scalatest.{ FlatSpec, BeforeAndAfter }
import org.apache.log4j.{ Logger, Level }
import com.intel.testutils.TestingSparkContextFlatSpec

class NormalizeConnectedComponentsTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait ConnectedComponentsTest {
    val vertexList: List[Long] = List(4, 8, 12, 16, 20, 24, 28, 32)
    val originalEquivalenceClasses: Set[Set[Long]] = Set(Set(8, 24, 32), Set(4, 16, 28), Set(12))
    val originalVertexToComponent: List[(Long, Long)] = List((8.toLong, 8.toLong),
      (24.toLong, 8.toLong), (32.toLong, 8.toLong), (4.toLong, 28.toLong), (16.toLong, 28.toLong),
      (28.toLong, 28.toLong), (12.toLong, 12.toLong))
  }

  "normalize connected components" should "not have any gaps in the range of component IDs returned" in new ConnectedComponentsTest {

    val verticesToComponentsRDD = sparkContext.parallelize(originalVertexToComponent).map(x => (x._1.toLong, x._2.toLong))
    val normalizeOut = NormalizeConnectedComponents.normalize(verticesToComponentsRDD, sparkContext)

    val normalizeOutComponents = normalizeOut._2.map(x => x._2).distinct()

    normalizeOutComponents.map(_.toLong).collect().toSet shouldEqual (1.toLong to (normalizeOutComponents.count().toLong)).toSet
  }

  "normalize connected components" should "create correct equivalence classes" in new ConnectedComponentsTest {

    val verticesToComponentsRDD = sparkContext.parallelize(originalVertexToComponent).map(x => (x._1.toLong, x._2.toLong))
    val normalizedCCROutput = NormalizeConnectedComponents.normalize(verticesToComponentsRDD, sparkContext)

    val vertexComponentPairs = normalizedCCROutput._2.collect()

    val groupedVertexComponentPairs = vertexComponentPairs.groupBy(x => x._2).map(x => x._2)

    val setsOfPairs = groupedVertexComponentPairs.map(list => list.toSet)

    val equivalenceclasses = setsOfPairs.map(set => set.map(x => x._1)).toSet

    equivalenceclasses shouldEqual originalEquivalenceClasses
  }

}
