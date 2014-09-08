package com.intel.intelanalytics.engine.spark.statistics

import com.intel.intelanalytics.engine.spark.statistics.TopKRDDFunctions.CountPair
import com.intel.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers

class TopKItest extends TestingSparkContextFlatSpec with Matchers {
  val inputList = List(
    Array[Any](-1, "a", 0),
    Array[Any](0, "c", 0),
    Array[Any](0, "b", 0),
    Array[Any](5, "b", 0),
    Array[Any](5, "b", 0),
    Array[Any](5, "a", 0)
  )

  val emptyList = List.empty[Array[Any]]

  val keyCountList = List[(Any, Long)](
    ("key1", 2),
    ("key2", 20),
    ("key3", 12),
    ("key4", 0),
    ("key5", 6))

  val emptyCountList = List.empty[(Any, Long)]

  "topK" should "return the top K distinct values sorted by count" in {
    val frameRdd = sparkContext.parallelize(inputList, 2)
    val top1Column0 = TopKRDDFunctions.topK(frameRdd, 0, 1, false).collect()

    top1Column0.size should equal(1)
    top1Column0(0) should equal(Array[Any](5, 3))
  }

  "topK" should "return all distinct values sorted by count if K exceeds input size" in {
    val frameRdd = sparkContext.parallelize(inputList, 2)
    val topKColumn1 = TopKRDDFunctions.topK(frameRdd, 1, 100, false).collect()

    topKColumn1.size should equal(3)
    topKColumn1(0) should equal(Array[Any]("b", 3))
    topKColumn1(1) should equal(Array[Any]("a", 2))
    topKColumn1(2) should equal(Array[Any]("c", 1))
  }

  "topK" should "return the bottom K distinct values sorted by count" in {
    val frameRdd = sparkContext.parallelize(inputList, 2)
    val bottom2Column1 = TopKRDDFunctions.topK(frameRdd, 1, 2, true).collect()

    bottom2Column1.size should equal(2)
    bottom2Column1(0) should equal(Array[Any]("c", 1))
    bottom2Column1(1) should equal(Array[Any]("a", 2))
  }

  "topK" should "return an empty sequence if the input data frame is empty" in {
    val frameRdd = sparkContext.parallelize(emptyList, 2)
    val bottomKColumn1 = TopKRDDFunctions.topK(frameRdd, 1, 4, true).collect()
    bottomKColumn1.size should equal(0)
  }

  "sortTopKByValue" should "return the top 3 entries by value sorted by descending order" in {
    val sortedK = TopKRDDFunctions.sortTopKByValue(keyCountList.toIterator, 3, descending = true).toSeq
    sortedK.size should equal(3)
    sortedK should equal(Seq(CountPair("key2", 20), CountPair("key3", 12), CountPair("key5", 6)))
  }

  "sortTopKByValue" should "return all entries sorted in descending order if K exceeds input size" in {
    val sortedK = TopKRDDFunctions.sortTopKByValue(keyCountList.toIterator, 10, descending = true).toSeq
    sortedK.size should equal(5)
    sortedK should equal(Seq(CountPair("key2", 20), CountPair("key3", 12), CountPair("key5", 6), CountPair("key1", 2), CountPair("key4", 0)))
  }

  "sortTopKByValue" should "return the top 2 entries by value in ascending order" in {
    val sortedK = TopKRDDFunctions.sortTopKByValue(keyCountList.toIterator, 2, descending = false).toSeq
    sortedK.size should equal(2)
    sortedK should equal(Seq(CountPair("key4", 0), CountPair("key1", 2)))
  }

  "sortTopKByValue" should "return empty if the input data is empty" in {
    val sortedK = TopKRDDFunctions.sortTopKByValue(emptyCountList.toIterator, 2, descending = false).toSeq
    sortedK.size should equal(0)
  }

}
