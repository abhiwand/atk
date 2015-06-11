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

package com.intel.intelanalytics.engine.spark.frame

import com.intel.intelanalytics.domain.HasData
import com.intel.intelanalytics.domain.frame.{ FrameMeta, FrameEntity, FrameReference }
import org.apache.spark.frame.FrameRdd

/**
 * A FrameReference with metadata and a Spark RDD representing the data in the frame.
 *
 * Note that in case the frame's schema is different from the rdd's, the rdd's wins.
 */
class SparkFrameData(frame: FrameEntity, rdd: FrameRdd)
    extends FrameMeta(frame.withSchema(rdd.frameSchema))
    with HasData {

  /**
   * Returns a copy with the given data instead of the current data
   */
  def withData(newData: FrameRdd): SparkFrameData = new SparkFrameData(this.meta, newData)

  type Data = FrameRdd

  val data = rdd

}
