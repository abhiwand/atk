//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.domain.model

import org.joda.time.DateTime
import org.scalatest.WordSpec

class ModelEntityTest extends WordSpec {

  val model = new ModelEntity(1, Some("name"), "model:subtype", None, 1, None, new DateTime(), new DateTime())

  "Model" should {

    "require an id greater than zero" in {
      intercept[IllegalArgumentException] { model.copy(id = -1) }
    }

    "require a name" in {
      intercept[IllegalArgumentException] { model.copy(name = null) }
    }

    "require a non-empty name" in {
      intercept[IllegalArgumentException] { model.copy(name = Some("")) }
    }

    "require a modelType" in {
      intercept[IllegalArgumentException] { model.copy(modelType = null) }
    }

    "require a modelType that starts with 'model:'" in {
      intercept[IllegalArgumentException] { model.copy(modelType = "frame") }
    }

  }

}
