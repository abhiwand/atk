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

import com.intel.intelanalytics.engine.TestingSparkContext
import org.scalatest.Matchers

import scala.collection.mutable.ArrayBuffer

class SparkOpsTest extends TestingSparkContext with Matchers {

  val max = 20
  val array = (1 to max * 2).map(i => Array(i, i.toString, i.toDouble * 0.1))

  def fetchAllData(): ArrayBuffer[Array[Any]] = {
    val data = sc.parallelize(array)
    val results = new ArrayBuffer[Array[Any]]()
    var offset = 0
    var loop = true
    while (loop) {
      val batch = SparkOps.getRows(data, offset, max, max)
      if (batch.length == 0)
        loop = false
      offset += max
      results ++= batch
    }
    results
  }

  "getRows" should "return the requested number of rows" in {
    val data = sc.parallelize(array)
    SparkOps.getRows(data, 0, max, max).length should equal(max)
  }

  it should "limit the returned rows based on configured restrictions" in {
    val data = sc.parallelize(array)
    SparkOps.getRows(data, 0, max + 5, max).length should equal(max)
  }

  it should "return no more rows than are available" in {
    val data = sc.parallelize(array)
    SparkOps.getRows(data, max * 2 - 5, max, max).length should equal(5)
  }

  it should "start at the requested offset" in {
    val data = sc.parallelize(array)
    SparkOps.getRows(data, max * 2 - 10, 5, max).length should equal(5)
  }

  it should "return no rows when a zero count is requested" in {
    val data = sc.parallelize(array)
    SparkOps.getRows(data, max * 2 - 10, 0, max).length should equal(0)
  }

  it should "return all the data when invoked enough times" in {
    val results = fetchAllData()

    results.length should equal(array.length)
  }

  it should "not generate the same row twice" in {
    val results = fetchAllData()

    results.groupBy { case Array(index, _, _) => index }.count(_._2.length > 1) should equal(0)
  }

  "getElements" should "be able to return non row objects" in {
    val data = sc.parallelize(List.range(0, 100))

    val results = SparkOps.getElements(data, 0, max, max)
    results(0).getClass should equal(Integer.TYPE)
  }
}
