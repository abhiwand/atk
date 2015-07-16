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

package com.intel.taproot.analytics.engine.spark.frame

import com.intel.taproot.analytics.domain.frame.{ FrameEntity, FrameReference }
import org.apache.spark.frame.FrameRdd

/**
 * A FrameReference with metadata and a Spark RDD representing the data in the frame.
 *
 * Note that in case the frame's schema is different from the rdd's, the rdd's wins.
 */
@deprecated("instead use FrameReference, FrameEntity, FrameRdd, SparkFrame")
class SparkFrameData(frame: FrameEntity, rdd: FrameRdd) {

  type Data = FrameRdd

  @deprecated("instead use FrameReference, FrameEntity, FrameRdd, SparkFrame")
  val data = rdd

}
