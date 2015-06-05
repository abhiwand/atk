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
