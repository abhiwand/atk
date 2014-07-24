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

package com.intel.spark.mllib.util

import org.apache.spark.SparkContext
import org.scalatest.{ BeforeAndAfterAll, FunSuite }
import org.scalatest.matchers.ShouldMatchers

import scala.util.Random

class MLDataSplitterSuite extends FunSuite with BeforeAndAfterAll with ShouldMatchers {
  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  // test if we can randomly split a RDD according to a percentage distribution
  test("MLDataSplitter") {

    val nPoints = 10000
    val percentages = Array(0.7, 0.1, 0.2)
    val labels = Array("bin #1", "bin #2", "bin #3")

    // generate testRDD
    val rnd = new Random(41)
    val testData = Array.fill[Double](nPoints)(rnd.nextGaussian())
    val testRDD = sc.parallelize(testData, 2)

    // test the size of generated RDD
    val nTotal = testRDD.count
    assert(nTotal == nPoints, "# data points generated isn't equal to specified.")

    // split the RDD by labelling
    val splitter = new MLDataSplitter(percentages, labels, 42)
    val labeledRDD = splitter.randomlyLabelRDD(testRDD)

    // collect the size of each partition
    val partitionSizes = new Array[Long](percentages.size)
    (0 until percentages.size).foreach { i =>
      val partitionRDD = labeledRDD.filter(p => p.label == labels.apply(i)).map(_.entry)
      partitionSizes(i) = partitionRDD.count
    }

    // test the total #samples
    val nTotalSamples = partitionSizes.sum
    assert(nTotalSamples == nPoints, "# data points sampled isn't equal to specified.")

    // check if partition percentages are expected 
    (0 until percentages.size).foreach { i =>
      val realPercentage = partitionSizes(i).toDouble / nTotalSamples
      assert(Math.abs(realPercentage - percentages(i)) < 0.05,
        "partition percentage isn't in [%f, %f].".format(percentages(i) - 0.05, percentages(i) + 0.05))
    }
  }

}
