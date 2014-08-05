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

package com.intel.testutils

import java.util.Date
import org.apache.spark.{ SparkConf, SparkContext }
import scala.concurrent.Lock

/**
 * This trait case be mixed into Specifications to create a SparkContext for testing.
 * <p>
 * IMPORTANT! This adds a couple seconds to your unit test!
 * </p>
 * <p>
 * Lock is used because you can only have one local SparkContext running at a time.
 * Other option is to use "parallelExecution in Test := false" but locking seems to be faster.
 * </p>
 * @deprecated we're switching to ScalaTest, shouldn't use Specs2 any more
 */
trait Specs2TestingSparkContext extends MultipleAfter {

  lazy val sc = TestingSparkContext.sparkContext

  /**
   * Clean up after the test is done
   */
  override def after: Any = {
    TestingSparkContext.cleanUp()
    super.after
  }

}