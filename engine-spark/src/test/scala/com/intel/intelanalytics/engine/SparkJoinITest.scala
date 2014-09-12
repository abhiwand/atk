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

package com.intel.intelanalytics.engine.spark

import org.scalatest.{ BeforeAndAfterEach, Matchers, FlatSpec }
import com.intel.testutils.TestingSparkContextFlatSpec
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.engine.spark.frame.RDDJoinParam

class SparkJoinITest extends TestingSparkContextFlatSpec with Matchers {

  "joinRDDs" should "join two RDD with inner join" in {
    val id_country_codes = List(Array[Any](1, 354), Array[Any](2, 91), Array[Any](3, 47), Array[Any](4, 968))
    val id_country_names = List(Array[Any](1, "Iceland"), Array[Any](2, "India"), Array[Any](3, "Norway"), Array[Any](4, "Oman"))

    val countryCode = sparkContext.parallelize(id_country_codes).map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }
    val countryNames = sparkContext.parallelize(id_country_names).map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }

    val result = SparkOps.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "inner")
    result.count shouldBe 4
    val sortable = result.map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }.asInstanceOf[RDD[(Int, Array[Any])]]
    val sorted = sortable.sortByKey(true)

    val data = sorted.take(4)
    data(0)._2 shouldBe Array(1, 354, 1, "Iceland")
    data(1)._2 shouldBe Array(2, 91, 2, "India")
    data(2)._2 shouldBe Array(3, 47, 3, "Norway")
    data(3)._2 shouldBe Array(4, 968, 4, "Oman")
  }

  "joinRDDs" should "join two RDD with left join" in {
    val id_country_codes = List(Array[Any](1, 354), Array[Any](2, 91), Array[Any](3, 47), Array[Any](4, 968))
    val id_country_names = List(Array[Any](1, "Iceland"), Array[Any](2, "India"), Array[Any](3, "Norway"))
    val countryCodeRDD: RDD[Array[Any]] = sparkContext.parallelize(id_country_codes)
    val countryCode = countryCodeRDD.map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }
    val countryNames = sparkContext.parallelize(id_country_names).map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }

    val result = SparkOps.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "left")
    result.count shouldBe 4
    val sortable = result.map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }.asInstanceOf[RDD[(Int, Array[Any])]]
    val sorted = sortable.sortByKey(true)

    val data = sorted.take(4)
    data(0)._2 shouldBe Array(1, 354, 1, "Iceland")
    data(1)._2 shouldBe Array(2, 91, 2, "India")
    data(2)._2 shouldBe Array(3, 47, 3, "Norway")
    data(3)._2 shouldBe Array(4, 968, null, null)
  }

  "joinRDDs" should "join two RDD with right join" in {
    val id_country_codes = List(Array[Any](1, 354), Array[Any](2, 91), Array[Any](3, 47))
    val id_country_names = List(Array[Any](1, "Iceland"), Array[Any](2, "India"), Array[Any](3, "Norway"), Array[Any](4, "Oman"))

    val countryCode = sparkContext.parallelize(id_country_codes).map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }
    val countryNames = sparkContext.parallelize(id_country_names).map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }

    val result = SparkOps.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "right")
    result.count shouldBe 4
    val sortable = result.map(t => SparkOps.createKeyValuePairFromRow(t, List(2))).map { case (keyColumns, data) => (keyColumns(0), data) }.asInstanceOf[RDD[(Int, Array[Any])]]
    val sorted = sortable.sortByKey(true)

    val data = sorted.take(4)

    data(0)._2 shouldBe Array(1, 354, 1, "Iceland")
    data(1)._2 shouldBe Array(2, 91, 2, "India")
    data(2)._2 shouldBe Array(3, 47, 3, "Norway")
    data(3)._2 shouldBe Array(null, null, 4, "Oman")
  }

  "joinRDDs" should "join two RDD with outer join" in {
    val id_country_codes = List(Array[Any](1, 354), Array[Any](2, 91), Array[Any](3, 47), Array[Any](5, 50))
    val id_country_names = List(Array[Any](1, "Iceland"), Array[Any](2, "India"), Array[Any](3, "Norway"), Array[Any](4, "Oman"))

    val countryCode = sparkContext.parallelize(id_country_codes).map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }
    val countryNames = sparkContext.parallelize(id_country_names).map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }

    val result = SparkOps.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "outer")
    result.count shouldBe 5
    val sortable = result.map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }.asInstanceOf[RDD[(Int, Array[Any])]]
    val sorted = sortable.sortByKey(true)

    val data = sorted.take(5)

    data(0)._2 shouldBe Array(null, null, 4, "Oman")
    data(1)._2 shouldBe Array(1, 354, 1, "Iceland")
    data(2)._2 shouldBe Array(2, 91, 2, "India")
    data(3)._2 shouldBe Array(3, 47, 3, "Norway")
    data(4)._2 shouldBe Array(5, 50, null, null)

  }

  "outer join with empty left RDD" should "preserve the result from the right RDD" in {
    val id_country_codes = List[Array[Any]]()
    val id_country_names = List(Array[Any](1, "Iceland"), Array[Any](2, "India"), Array[Any](3, "Norway"), Array[Any](4, "Oman"))

    val countryCode = sparkContext.parallelize(id_country_codes).map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }
    val countryNames = sparkContext.parallelize(id_country_names).map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }

    val result = SparkOps.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "outer")
    result.count shouldBe 4

    val sortable = result.map(t => SparkOps.createKeyValuePairFromRow(t, List(2))).map { case (keyColumns, data) => (keyColumns(0), data) }.asInstanceOf[RDD[(Int, Array[Any])]]
    val sorted = sortable.sortByKey(true)

    val data = sorted.take(4)

    data(0)._2 shouldBe Array(null, null, 1, "Iceland")
    data(1)._2 shouldBe Array(null, null, 2, "India")
    data(2)._2 shouldBe Array(null, null, 3, "Norway")
    data(3)._2 shouldBe Array(null, null, 4, "Oman")
  }

  "outer join with empty right RDD" should "preserve the result from the left RDD" in {
    val id_country_codes = List(Array[Any](1, 354), Array[Any](2, 91), Array[Any](3, 47), Array[Any](5, 50))
    val id_country_names = List[Array[Any]]()

    val countryCode = sparkContext.parallelize(id_country_codes).map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }
    val countryNames = sparkContext.parallelize(id_country_names).map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }

    val result = SparkOps.joinRDDs(RDDJoinParam(countryCode, 2), RDDJoinParam(countryNames, 2), "outer")
    result.count shouldBe 4

    val sortable = result.map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }.asInstanceOf[RDD[(Int, Array[Any])]]
    val sorted = sortable.sortByKey(true)

    val data = sorted.take(4)

    data(0)._2 shouldBe Array(1, 354, null, null)
    data(1)._2 shouldBe Array(2, 91, null, null)
    data(2)._2 shouldBe Array(3, 47, null, null)
    data(3)._2 shouldBe Array(5, 50, null, null)
  }

  "outer join large RDD" should "generate RDD contains all element from both RDD" in {
    val oneToMillon = (1 to 1000000).map(i => Array[Any](i))
    val fiveHundredThousandsToOneFiftyThousands = (500001 to 1500000).map(i => Array[Any](i))

    val rddOneToMillon = sparkContext.parallelize(oneToMillon).map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }
    val rddFiveHundredThousandsToOneFiftyThousands = sparkContext.parallelize(fiveHundredThousandsToOneFiftyThousands).map(t => SparkOps.createKeyValuePairFromRow(t, List(0))).map { case (keyColumns, data) => (keyColumns(0), data) }

    val rddFullOuterJoin = SparkOps.joinRDDs(RDDJoinParam(rddOneToMillon, 1), RDDJoinParam(rddFiveHundredThousandsToOneFiftyThousands, 1), "outer")
    rddFullOuterJoin.count shouldBe 1500000
  }

}
