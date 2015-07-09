/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.analytics.engine.spark.frame.plugins.topk

import com.intel.taproot.analytics.domain.schema.DataTypes
import com.intel.taproot.analytics.engine.spark.frame.plugins.topk.TopKRddFunctions.CountPair
import com.intel.taproot.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers

class TopKItest extends TestingSparkContextFlatSpec with Matchers {
  val inputList = List(
    Array[Any](-1, "a", 0, 2d, 0d),
    Array[Any](0, "c", 0, 1d, 0d),
    Array[Any](0, "b", 0, 0.5d, 0d),
    Array[Any](5, "b", 0, 0.25d, 0d),
    Array[Any](5, "b", 0, 0.2d, 0d),
    Array[Any](5, "a", 0, 0.1d, 0d)
  )

  val emptyList = List.empty[Array[Any]]

  val keyCountList = List[(Any, Double)](
    ("key1", 2),
    ("key2", 20),
    ("key3", 12),
    ("key4", 0),
    ("key5", 6))

  val emptyCountList = List.empty[(Any, Double)]

  "topK" should "return the top K distinct values sorted by count" in {
    val frameRdd = sparkContext.parallelize(inputList, 2)
    val top1Column0 = TopKRddFunctions.topK(frameRdd, 0, 1, false).collect()

    top1Column0.size should equal(1)
    top1Column0(0) should equal(Array[Any](5, 3))
  }

  "topK" should "return all top K distinct values sorted by count if K exceeds input size" in {
    val frameRdd = sparkContext.parallelize(inputList, 2)
    val topKColumn1 = TopKRddFunctions.topK(frameRdd, 1, 100, false).collect()

    topKColumn1.size should equal(3)
    topKColumn1(0) should equal(Array[Any]("b", 3))
    topKColumn1(1) should equal(Array[Any]("a", 2))
    topKColumn1(2) should equal(Array[Any]("c", 1))
  }

  "topK" should "return the weighted top K distinct values sorted by count" in {
    val frameRdd = sparkContext.parallelize(inputList, 2)
    val topKColumn1 = TopKRddFunctions.topK(frameRdd, 1, 3, false, Some(3), Some(DataTypes.float64)).collect()

    topKColumn1.size should equal(3)
    topKColumn1(0) should equal(Array[Any]("a", 2.1))
    topKColumn1(1) should equal(Array[Any]("c", 1))
    topKColumn1(2) should equal(Array[Any]("b", 0.95))
  }

  "topK" should "return the bottom K distinct values sorted by count" in {
    val frameRdd = sparkContext.parallelize(inputList, 2)
    val bottom2Column1 = TopKRddFunctions.topK(frameRdd, 1, 2, true).collect()

    bottom2Column1.size should equal(2)
    bottom2Column1(0) should equal(Array[Any]("c", 1))
    bottom2Column1(1) should equal(Array[Any]("a", 2))
  }

  "topK" should "return the weighted bottom K distinct values sorted by count" in {
    val frameRdd = sparkContext.parallelize(inputList, 2)
    val topKColumn1 = TopKRddFunctions.topK(frameRdd, 1, 3, true, Some(3), Some(DataTypes.float64)).collect()

    topKColumn1.size should equal(3)
    topKColumn1(0) should equal(Array[Any]("b", 0.95))
    topKColumn1(1) should equal(Array[Any]("c", 1))
    topKColumn1(2) should equal(Array[Any]("a", 2.1))
  }

  "topK" should "return an empty sequence if the input data frame is empty" in {
    val frameRdd = sparkContext.parallelize(emptyList, 2)
    val bottomKColumn1 = TopKRddFunctions.topK(frameRdd, 1, 4, true).collect()
    bottomKColumn1.size should equal(0)
  }

  "topK" should "return an empty sequence if all columns have zero weight" in {
    val frameRdd = sparkContext.parallelize(inputList, 2)
    val topKColumn1 = TopKRddFunctions.topK(frameRdd, 1, 2, false, Some(4), Some(DataTypes.float64)).collect()
    topKColumn1.size should equal(0)
  }

  "sortTopKByValue" should "return the top 3 entries by value sorted by descending order" in {
    val sortedK = TopKRddFunctions.sortTopKByValue(keyCountList.toIterator, 3, descending = true).toSeq
    sortedK.size should equal(3)
    sortedK should equal(Seq(CountPair("key2", 20), CountPair("key3", 12), CountPair("key5", 6)))
  }

  "sortTopKByValue" should "return all entries sorted in descending order if K exceeds input size" in {
    val sortedK = TopKRddFunctions.sortTopKByValue(keyCountList.toIterator, 10, descending = true).toSeq
    sortedK.size should equal(5)
    sortedK should equal(Seq(CountPair("key2", 20), CountPair("key3", 12), CountPair("key5", 6), CountPair("key1", 2), CountPair("key4", 0)))
  }

  "sortTopKByValue" should "return the top 2 entries by value in ascending order" in {
    val sortedK = TopKRddFunctions.sortTopKByValue(keyCountList.toIterator, 2, descending = false).toSeq
    sortedK.size should equal(2)
    sortedK should equal(Seq(CountPair("key4", 0), CountPair("key1", 2)))
  }

  "sortTopKByValue" should "return empty if the input data is empty" in {
    val sortedK = TopKRddFunctions.sortTopKByValue(emptyCountList.toIterator, 2, descending = false).toSeq
    sortedK.size should equal(0)
  }

}