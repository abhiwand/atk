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

package com.intel.spark.graphon.sampling

import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar
import com.intel.intelanalytics.domain.graph.GraphReference

class VertexSampleArgumentsTest extends WordSpec with MockitoSugar {

  "VertexSampleArguments" should {
    "require sample size greater than or equal to 1" in {
      VertexSampleArguments(mock[GraphReference], 1, "uniform")
    }

    "require sample size greater than 0" in {
      intercept[IllegalArgumentException] { VertexSampleArguments(mock[GraphReference], 0, "uniform") }
    }

    "require a valid sample type" in {
      intercept[IllegalArgumentException] { VertexSampleArguments(mock[GraphReference], 2, "badvalue") }
    }
  }
}
