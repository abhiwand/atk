package com.intel.intelanalytics.repository

import com.intel.intelanalytics.domain.model.ModelTemplate
import org.scalatest.Matchers

class ModelRepositoryTest extends SlickMetaStoreH2Testing with Matchers {

  "ModelRepository" should "be able to create models" in {
    val modelRepo = slickMetaStoreComponent.metaStore.modelRepo
    slickMetaStoreComponent.metaStore.withSession("model-test") {
      implicit session =>
        val name = "my-model"
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
