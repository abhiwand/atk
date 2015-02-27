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
