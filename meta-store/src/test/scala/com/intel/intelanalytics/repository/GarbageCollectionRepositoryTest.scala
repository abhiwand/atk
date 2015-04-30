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

import com.intel.intelanalytics.domain.gc.{ GarbageCollection, GarbageCollectionTemplate }
import com.intel.intelanalytics.domain.model.ModelTemplate
import org.joda.time.DateTime
import org.scalatest.Matchers

class GarbageCollectionRepositoryTest extends SlickMetaStoreH2Testing with Matchers {

  "GarbageCollectionRepository" should "be able to create new items" in {
    val gcRepo = slickMetaStoreComponent.metaStore.gcRepo
    slickMetaStoreComponent.metaStore.withSession("gc-test") {
      implicit session =>

        //create gc
        val startTime: DateTime = new DateTime
        val template = new GarbageCollectionTemplate("hostname", 100, startTime)

        val gc = gcRepo.insert(template)
        gc.get should not be null

        //look it up
        val gc2: Option[GarbageCollection] = gcRepo.lookup(gc.get.id)
        gc2.get should not be null
        gc2.get.hostname should be("hostname")
        gc2.get.processId should be(100)
        gc2.get.startTime.getMillis should be(startTime.getMillis)
        gc2.get.endTime should be(None)
    }

  }

}
