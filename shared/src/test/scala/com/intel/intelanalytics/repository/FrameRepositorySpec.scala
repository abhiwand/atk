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

import org.scalatest.Matchers
import com.intel.intelanalytics.domain.frame.DataFrameTemplate

class FrameRepositorySpec extends SlickMetaStoreH2Testing with Matchers {

  "FrameRepository" should "be able to create frames" in {
    val frameRepo = slickMetaStoreComponent.metaStore.frameRepo
    slickMetaStoreComponent.metaStore.withSession("frame-test") {
      implicit session =>

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
