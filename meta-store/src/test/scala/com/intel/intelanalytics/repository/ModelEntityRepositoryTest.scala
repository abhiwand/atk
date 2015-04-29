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

package com.intel.intelanalytics.repository

import com.intel.event.EventContext
import com.intel.intelanalytics.domain.graph.GraphTemplate
import com.intel.intelanalytics.domain.model.ModelTemplate
import org.joda.time.DateTime
import org.scalatest.Matchers

class ModelEntityRepositoryTest extends SlickMetaStoreH2Testing with Matchers {

  "ModelRepository" should "be able to create models" in {
    val modelRepo = slickMetaStoreComponent.metaStore.modelRepo
    slickMetaStoreComponent.metaStore.withSession("model-test") {
      implicit session =>
        val name = Some("my_model")
        val modelType = "model:logistic_regression"

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

  it should "return a list of graphs ready to have their data deleted" in {
    val modelRepo = slickMetaStoreComponent.metaStore.modelRepo
    slickMetaStoreComponent.metaStore.withSession("model-test") {
      implicit session =>
        val age = 10 * 24 * 60 * 60 * 1000 //10 days

        val name = Some("my_model")
        val modelType = "model:logistic_regression"

        // create graphs

        //should be in list old and unnamed
        val model1 = modelRepo.insert(new ModelTemplate(None, modelType)).get
        modelRepo.update(model1.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should not be in list. it is named
        val model2 = modelRepo.insert(new ModelTemplate(name, modelType)).get
        modelRepo.update(model2.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should not be in list. it is too new
        val model3 = modelRepo.insert(new ModelTemplate(None, modelType)).get
        modelRepo.update(model3.copy(lastReadDate = new DateTime()))

        //should  be in list as user marked it as deleted
        val model5 = modelRepo.insert(new ModelTemplate(name, modelType)).get
        modelRepo.update(model5.copy(statusId = DeletedStatus))

        //should not be in list it has already been deleted
        val model6 = modelRepo.insert(new ModelTemplate(name, modelType)).get
        modelRepo.update(model6.copy(statusId = DeletedFinalStatus))

        val readyForDeletion = modelRepo.listReadyForDeletion(age)
        readyForDeletion.length should be(2)
        val idList = readyForDeletion.map(m => m.id).toList
        idList should contain(model1.id)
    }
  }

}
