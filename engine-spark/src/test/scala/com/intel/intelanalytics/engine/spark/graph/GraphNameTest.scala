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

package com.intel.intelanalytics.engine.spark.graph

import org.scalatest.{ Matchers, WordSpec }

class GraphNameTest extends WordSpec with Matchers {

  "ConvertGraphUserNameToBackendName" should {
    "preface user name with expected string" in {
      val userName = "Graph of the Gods"
      val expectedBackendName = "iat_graph_" + userName
      GraphBackendName.convertGraphUserNameToBackendName(userName) shouldEqual expectedBackendName
    }
  }

  "ConvertGraphBackendNameToUserName" should {
    "strip expected string from the backend name" in {
      val expectedUserName = "VALHALLA! I AM COMING!!!"
      val backendName = "iat_graph_" + expectedUserName
      GraphBackendName.convertGraphBackendNameToUserName(backendName) shouldEqual expectedUserName
    }
  }
}
