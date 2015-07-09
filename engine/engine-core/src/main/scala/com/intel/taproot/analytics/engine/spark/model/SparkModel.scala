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

package com.intel.taproot.analytics.engine.spark.model

import com.intel.taproot.analytics.domain.model.{ ModelMeta, ModelEntity }
import com.intel.taproot.analytics.domain.HasData
import org.apache.spark.frame.FrameRdd

class SparkModel(model: ModelEntity)
    extends ModelMeta(model)
    with HasData {

  /**
   * Returns a copy with the given data instead of the current data
   */
  def withData(): SparkModel = new SparkModel(this.meta)

  type Data = FrameRdd // bogus, Model has no data RDD

  val data = null
}