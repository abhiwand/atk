package com.intel.intelanalytics.repository

import org.scalatest.Matchers
import com.intel.intelanalytics.domain.frame.DataFrameTemplate

class FrameRepositorySpec extends SlickMetaStoreH2Testing with Matchers {

  "FrameRepository" should "be able to create frames" in {
    val frameRepo = slickMetaStoreComponent.metaStore.frameRepo
    slickMetaStoreComponent.metaStore.withSession("frame-test") {
      implicit session ⇒

        val frameName = "frame-name"
        val frameDescription = "my description"

        // create a frame
        val frame = frameRepo.insert(new DataFrameTemplate(frameName, Some(frameDescription)))
        frame.get should not be null

        // look it up and validate expected values
        val frame2 = frameRepo.lookup(frame.get.id)
        frame2.get should not be null
        frame2.get.name shouldBe frameName
        frame2.get.description.get shouldBe frameDescription
        frame2.get.status shouldBe 1
        frame2.get.createdOn should not be null
        frame2.get.modifiedOn should not be null
    }
  }

}
