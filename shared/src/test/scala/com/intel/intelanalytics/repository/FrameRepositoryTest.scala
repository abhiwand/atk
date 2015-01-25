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

import java.util.concurrent.TimeUnit

import com.intel.intelanalytics.domain.StorageFormats
import com.intel.intelanalytics.domain.frame.{ DataFrameTemplate }
import com.intel.intelanalytics.domain.graph.{ GraphEntity, GraphTemplate }
import com.typesafe.config.ConfigFactory
import org.joda.time.{ Duration, DateTime }
import org.scalatest.Matchers

class FrameRepositoryTest extends SlickMetaStoreH2Testing with Matchers {

  "FrameRepository" should "be able to create frames" in {
    val frameRepo = slickMetaStoreComponent.metaStore.frameRepo
    slickMetaStoreComponent.metaStore.withSession("frame-test") {
      implicit session =>

        val frameName = Some("frame_name")
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

  it should "be able to update errorFrameIds in" in {
    val frameRepo = slickMetaStoreComponent.metaStore.frameRepo
    slickMetaStoreComponent.metaStore.withSession("frame-test") {
      implicit session =>

        val frameName = "frame_name"

        // create the frames
        val frame = frameRepo.insert(new DataFrameTemplate(Some(frameName), None)).get
        val errorFrame = frameRepo.insert(new DataFrameTemplate(Some(frameName + "_errors"), None)).get

        // invoke method under test
        frameRepo.updateErrorFrameId(frame, Some(errorFrame.id))

        // look it up and validate expected values
        val frame2 = frameRepo.lookup(frame.id)
        frame2.get.errorFrameId.get shouldBe errorFrame.id
    }
  }

  it should "return a list of frames ready to have their data deleted" in {
    val frameRepo = slickMetaStoreComponent.metaStore.frameRepo
    val graphRepo = slickMetaStoreComponent.metaStore.graphRepo
    slickMetaStoreComponent.metaStore.withSession("frame-test") {
      implicit session =>

        val age = 10 * 24 * 60 * 60 * 1000 //10 days

        val frameName = Some("frame_name")

        // create the frames
        // should not be in list    too new
        val frame = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame.copy(lastReadDate = new DateTime))

        //should be in list old and unreferenced
        val frame2 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame2.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should be in list old things that are referenced can be weakly live
        val frame3 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame3.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should not be in list. it is named
        val frame4 = frameRepo.insert(new DataFrameTemplate(frameName, None)).get
        frameRepo.update(frame4.copy(lastReadDate = new DateTime().minus(age * 2), parent = Some(frame3.id)))

        val seamlessWeak: GraphEntity = graphRepo.insert(new GraphTemplate(None)).get
        val seamlessLive: GraphEntity = graphRepo.insert(new GraphTemplate(Some("liveGraph"))).get

        //should not be in list. it is too new
        val frame5 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame5.copy(lastReadDate = new DateTime(), graphId = Some(seamlessWeak.id)))

        //should be in list. it is old and referenced by a weakly live graph
        val frame6 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame6.copy(lastReadDate = new DateTime().minus(age * 2), graphId = Some(seamlessWeak.id)))

        //should not be in list. it is old but referenced by a live graph
        val frame7 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame7.copy(lastReadDate = new DateTime().minus(age * 2), graphId = Some(seamlessLive.id)))

        frameRepo.listReadyForDeletion(age).length should be(3)
    }
  }

  it should "return a list of frames ready to have their metadata deleted" in {
    val frameRepo = slickMetaStoreComponent.metaStore.frameRepo
    val graphRepo = slickMetaStoreComponent.metaStore.graphRepo
    slickMetaStoreComponent.metaStore.withSession("frame-test") {
      implicit session =>
        val age = 10 * 24 * 60 * 60 * 1000 //10 days

        val frameName = Some("frame_name")

        // create the frames
        // should not be in list too new
        val frame = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame.copy(lastReadDate = new DateTime, status = 8))

        //should not be in list old and wrong status
        val frame2 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame2.copy(lastReadDate = new DateTime().minus(age * 2), status = 7))

        //should be in list old and right status
        val frame3 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame3.copy(lastReadDate = new DateTime().minus(age * 2), status = 8))

        //should not be in list. it is already deleted
        val frame4 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame4.copy(lastReadDate = new DateTime().minus(age * 2), status = 4))

        //should not be in list. it is a parent of a living frame. see below
        val frame5 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame5.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should not be in list. it is live
        val frame6 = frameRepo.insert(new DataFrameTemplate(frameName, None)).get
        frameRepo.update(frame6.copy(lastReadDate = new DateTime().minus(age * 2), parent = Some(frame5.id)))

        val seamlessWeak: GraphEntity = graphRepo.insert(new GraphTemplate(None)).get
        val seamlessLive: GraphEntity = graphRepo.insert(new GraphTemplate(Some("liveGraph"))).get

        //should not be in list. it is too new
        val frame7 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame7.copy(lastReadDate = new DateTime(), graphId = Some(seamlessWeak.id), status = 8))

        //should be in list. it is old and referenced by a weakly live graph
        val frame8 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame8.copy(lastReadDate = new DateTime().minus(age * 2), graphId = Some(seamlessWeak.id), status = 8))

        //should not be in list. it is old but referenced by a live graph
        val frame9 = frameRepo.insert(new DataFrameTemplate(None, None)).get
        frameRepo.update(frame9.copy(lastReadDate = new DateTime().minus(age * 2), graphId = Some(seamlessLive.id), status = 8))

        frameRepo.listReadyForMetaDataDeletion(age).length should be(2)
    }
  }
}
