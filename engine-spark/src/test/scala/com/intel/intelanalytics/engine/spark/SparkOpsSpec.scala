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

import org.scalatest.{Suites, Matchers, FlatSpec}
import com.intel.intelanalytics.engine.TestingSparkContext
import com.typesafe.config.ConfigFactory
import org.scalatest.mock.MockitoSugar

class SparkOpsSpec extends Suites(GetRows) with Matchers {

  //TODO
//  val config = ConfigFactory.load("engine.conf")
//  val max = config.getInt("intel.analytics.engine.max-rows")
  val max = 20
  val array = (1 to max * 2).map(i => Array(i, i.toString, i.toDouble * 0.1))
  val data = sc.parallelize(array)

  "getRows" should "return the requested number of rows" in {
    SparkOps.getRows(data, 0, max).length should equal(max)
  }

//TODO
//  it should "limit the returned rows based on configured restrictions" in {
//    SparkOps.getRows(data, 0, max + 5).length should equal(max)
//  }

  it should "return no more rows than are available" in {
    SparkOps.getRows(data, max * 2 - 5, max) should equal(5)
  }

  it should "start at the requested offset" in {
    SparkOps.getRows(data, max * 2 - 10, 5).length should equal(5)
  }

}
