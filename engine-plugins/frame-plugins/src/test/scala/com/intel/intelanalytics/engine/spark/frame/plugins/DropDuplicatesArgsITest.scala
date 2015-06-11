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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.engine.spark.frame.MiscFrameFunctions
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.scalatest.{ BeforeAndAfterEach, FlatSpec, Matchers }

class DropDuplicatesArgsITest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContextFlatSpec {
  "removeDuplicatesByKey" should "keep only 1 rows per key" in {

    //setup test data
    val favoriteMovies = List(Array[Any]("John", 1, "Titanic"), Array[Any]("Kathy", 2, "Jurassic Park"), Array[Any]("John", 1, "The kite runner"), Array[Any]("Kathy", 2, "Toy Story 3"), Array[Any]("Peter", 3, "Star War"))
    val rdd = sparkContext.parallelize(favoriteMovies)

    rdd.count() shouldBe 5

    //prepare a pair rdd for removing duplicates
    val pairRdd = rdd.map(row => MiscFrameFunctions.createKeyValuePairFromRow(row, Seq(0, 1)))

    //remove duplicates identified by key
    val duplicatesRemoved = MiscFrameFunctions.removeDuplicatesByKey(pairRdd)
    duplicatesRemoved.count() shouldBe 3 // original data contain 5 rows, now drop to 3

    //transform output to a sortable format
    val sortable = duplicatesRemoved.map(t => MiscFrameFunctions.createKeyValuePairFromRow(t, Seq(1))).map { case (keyColumns, data) => (keyColumns(0), data) }.asInstanceOf[RDD[(Int, Array[Any])]]

    //sort output to validate result
    val sorted = sortable.sortByKey(true)

    //matching the result
    val data = sorted.take(4)
    data(0)._2 shouldBe Array[Any]("John", 1, "Titanic")
    data(1)._2 shouldBe Array[Any]("Kathy", 2, "Jurassic Park")
    data(2)._2 shouldBe Array[Any]("Peter", 3, "Star War")
  }
}
