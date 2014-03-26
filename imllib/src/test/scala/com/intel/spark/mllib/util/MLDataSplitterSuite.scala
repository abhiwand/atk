//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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

import scala.util.Random
import scala.collection.JavaConversions._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression._

class MLDataSplitterSuite extends FunSuite with BeforeAndAfterAll with ShouldMatchers {
  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
  }


  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  // Test if we can randomly split a RDD according to a percentage distribution
  test("MLDataSplitter") {
        
    val nPoints = 10000
    val percentageStr = "tr:0.7,va:0.1,te:0.2"
    val percentages = percentageStr.split(',').map(_.split(':'))

    val partitionPercentages = percentages.map(_(1).toDouble)    
    val splitter = new MLDataSplitter(partitionPercentages, 41)

    val rnd = new Random(42)
    val testData = Array.fill[String](nPoints)(rnd.nextGaussian().toString())
    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()
    
    // Test the size of generated RDD
    val nTotal = testRDD.count
    assert(nTotal == nPoints, "#data points generated isn't equal to specified.")
    
    // Split the RDD with labels
    val labeledRDD = splitter.randomlyLabelRDD(testRDD)
    
    // Collect the size of each partition
    val partitionSizes = new Array[Double](percentages.size)
    (0 until percentages.size).foreach { i =>
      val partitionRDD = labeledRDD.filter(p => p.label == i).map(_.entry)
      partitionSizes(i) = partitionRDD.count
    }

    // Test the total samples size
    val nTotalSamples = partitionSizes.reduceLeft(_ + _)
    assert(nTotalSamples == nPoints, "#data points sampled isn't equal to specified.")
    
    // Check if partition percentages are as expected 
    (0 until percentages.size).foreach { i =>
      val realPercentages = partitionSizes(i).toDouble / nTotalSamples 
      assert( realPercentages >= partitionPercentages(i) - 0.05 && realPercentages <= partitionPercentages(i) + 0.05,
        "percentage isn't in [%f, %f].".format(partitionPercentages(i) - 0.05, partitionPercentages(i) + 0.05))    
    }
  }

}
