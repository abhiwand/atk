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

package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.domain.HasData
import com.intel.intelanalytics.domain.frame.{ FrameMeta, FrameEntity, FrameReference }

/**
 * A FrameReference with metadata and a Spark RDD representing the data in the frame.
 *
 * Note that in case the frame's schema is different from the rdd's, the rdd's wins.
 */
class SparkFrameData(frame: FrameEntity, rdd: FrameRDD)
    extends FrameMeta(frame.withSchema(rdd.frameSchema))
    with HasData {

  /**
   * Returns a copy with the given data instead of the current data
   */
  def withData(newData: FrameRDD): SparkFrameData = new SparkFrameData(this.meta, newData)

  type Data = FrameRDD

  val data = rdd

}
