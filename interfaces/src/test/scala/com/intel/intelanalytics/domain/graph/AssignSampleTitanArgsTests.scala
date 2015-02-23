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

package com.intel.intelanalytics.domain.graph

import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, WordSpec }

class AssignSampleTitanArgsTests extends WordSpec with Matchers with MockitoSugar {

  "AssignSampleTitanArgs" should {
    "get a default Random seed" in {
      val args = new AssignSampleTitanArgs(mock[GraphReference], List(1), None, None, None)
      args.getRandomSeed should be(0)
    }
    "get default labels" in {
      val args = new AssignSampleTitanArgs(mock[GraphReference], List(0.3, 0.3, 0.2, 0.2), None, None, None)
      args.getSampleLabels should be(List("Sample_0", "Sample_1", "Sample_2", "Sample_3"))
    }
    "create a special label list if there are three percentages" in {
      val args = new AssignSampleTitanArgs(mock[GraphReference], List(0.3, 0.3, 0.3), None, None, None)
      args.getSampleLabels should be(List("TR", "TE", "VA"))
    }
    "get a default output property" in {
      val args = new AssignSampleTitanArgs(mock[GraphReference], List(0.3, 0.3, 0.3), None, None, None)
      args.getOutputProperty should be("sample_bin")
    }

    "require a frame reference" in {
      intercept[IllegalArgumentException] { AssignSampleTitanArgs(null, List(0)) }
    }

    "require sample percentages " in {
      intercept[IllegalArgumentException] { AssignSampleTitanArgs(mock[GraphReference], null) }
    }

    "require sample percentages greater than zero" in {
      intercept[IllegalArgumentException] { AssignSampleTitanArgs(mock[GraphReference], List(-1)) }
    }

    "require sample percentages sum less than one" in {
      intercept[IllegalArgumentException] { AssignSampleTitanArgs(mock[GraphReference], List(.33, .33, .5)) }
    }
  }

}
