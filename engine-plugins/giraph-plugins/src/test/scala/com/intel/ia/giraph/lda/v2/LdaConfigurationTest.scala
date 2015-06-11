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

package com.intel.ia.giraph.lda.v2

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.model.ModelReference
import com.intel.intelanalytics.domain.schema.FrameSchema
import org.apache.hadoop.conf.Configuration
import org.scalatest.WordSpec

class LdaConfigurationTest extends WordSpec {

  "LdaConfiguration" should {

    "require a value to be set" in {
      val config = new LdaConfiguration(new Configuration())
      intercept[IllegalArgumentException] {
        config.validate()
      }
    }

    "support ldaConfig json serialization/deserialization" in {
      val config = new LdaConfiguration(new Configuration())
      val ldaInputConfig = new LdaInputFormatConfig("input-location", new FrameSchema())
      val ldaOutputConfig = new LdaOutputFormatConfig("doc-results", "word-results")
      val ldaArgs = new LdaTrainArgs(new ModelReference(1), new FrameReference(2), "doc", "word", "word_count")
      val ldaConfig = new LdaConfig(ldaInputConfig, ldaOutputConfig, ldaArgs)
      config.setLdaConfig(ldaConfig)
      assert(config.ldaConfig.toString == ldaConfig.toString, "ldaConfig was not the same after deserialization")
    }
  }
}
