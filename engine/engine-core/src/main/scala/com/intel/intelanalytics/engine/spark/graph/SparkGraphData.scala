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

package com.intel.intelanalytics.engine.spark.graph

import com.intel.intelanalytics.domain.HasData
import com.intel.intelanalytics.domain.graph.{ GraphMeta, GraphEntity }
import org.apache.spark.frame.FrameRdd

/**
 * A GraphReference with metadata and a Spark RDD representing the data in the frame
 */
class SparkGraphData(graph: GraphEntity, rdd: Option[FrameRdd])
    extends GraphMeta(graph)
    with HasData {

  type Data = Option[FrameRdd]

  val data = rdd

}
