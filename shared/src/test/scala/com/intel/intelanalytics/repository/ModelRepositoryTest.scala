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

package com.intel.intelanalytics.repository

import com.intel.event.EventContext
import com.intel.intelanalytics.domain.model.ModelTemplate
import org.scalatest.Matchers

class ModelRepositoryTest extends SlickMetaStoreH2Testing with Matchers {

  "ModelRepository" should "be able to create models" in {
    val modelRepo = slickMetaStoreComponent.metaStore.modelRepo
    slickMetaStoreComponent.metaStore.withSession("model-test") {
      implicit session =>
        val name = "my_model"
        val modelType = "LogisticRegression"

        // create a model
        val model = modelRepo.insert(new ModelTemplate(name, modelType))
        model.get should not be null

        //look it up and validate expected values
        val model2 = modelRepo.lookup(model.get.id)
        model2.get should not be null
        model2.get.name shouldBe name
        model2.get.createdOn should not be null
        model2.get.modifiedOn should not be null

    }

  }

}
