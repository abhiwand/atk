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

import com.intel.intelanalytics.engine.plugin.Call
import org.scalatest.FlatSpec

class FrameFileStorageTest extends FlatSpec {

  implicit val call = Call(null)
  val frameFileStorage = new FrameFileStorage("hdfs://hostname/user/iauser", null)

  "FrameFileStorage" should "determine the correct data frames base directory" in {
    assert(frameFileStorage.frameBaseDirectory(1L).toString == "hdfs://hostname/user/iauser/intelanalytics/dataframes/1")
  }

}
