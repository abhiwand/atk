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
import com.intel.intelanalytics.domain.frame.DataFrameTemplate
import org.joda.time.DateTime
import org.scalatest.Matchers
import com.intel.intelanalytics.domain.graph.GraphTemplate

class GraphRepositoryTest extends SlickMetaStoreH2Testing with Matchers {

  "GraphRepository" should "be able to create graphs" in {
    val graphRepo = slickMetaStoreComponent.metaStore.graphRepo
    slickMetaStoreComponent.metaStore.withSession("graph-test") {
      implicit session =>

        val name = Some("my_name")

        // create a graph
        val graph = graphRepo.insert(new GraphTemplate(name, "hbase/titan"))
        graph.get should not be null

        // look it up and validate expected values
        val graph2 = graphRepo.lookup(graph.get.id)
        graph2.get should not be null
        graph2.get.name shouldBe name
        graph2.get.createdOn should not be null
        graph2.get.modifiedOn should not be null
    }
  }

  it should "return a list of graphs ready to have their data deleted" in {
    val frameRepo = slickMetaStoreComponent.metaStore.frameRepo
    val graphRepo = slickMetaStoreComponent.metaStore.graphRepo
    slickMetaStoreComponent.metaStore.withSession("graph-test") {
      implicit session =>
        val age = 10 * 24 * 60 * 60 * 1000 //10 days

        val name = Some("my_name")

        // create graphs

        //should be in list old and unnamed
        val graph1 = graphRepo.insert(new GraphTemplate(None)).get
        graphRepo.update(graph1.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should not be in list. it is named
        val graph2 = graphRepo.insert(new GraphTemplate(name)).get
        graphRepo.update(graph2.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should not be in list. it is too new
        val graph3 = graphRepo.insert(new GraphTemplate(None)).get
        graphRepo.update(graph3.copy(lastReadDate = new DateTime()))

        //should not be in list has a named frame as part of it.
        val graph4 = graphRepo.insert(new GraphTemplate(None)).get
        graphRepo.update(graph4.copy(lastReadDate = new DateTime().minus(age * 2)))

        val frame = frameRepo.insert(new DataFrameTemplate(Some("namedFrame"), None)).get
        frameRepo.update(frame.copy(lastReadDate = new DateTime().minus(age * 2), graphId = Some(graph4.id)))

        //should be in list. user has marked as ready to delete
        val graph5 = graphRepo.insert(new GraphTemplate(name)).get
        graphRepo.update(graph5.copy(statusId = DeletedStatus))

        //should not be in list. Has already been deleted
        val graph6 = graphRepo.insert(new GraphTemplate(name)).get
        graphRepo.update(graph6.copy(statusId = DeletedFinalStatus))

        val readyForDeletion = graphRepo.listReadyForDeletion(age)
        val idList = readyForDeletion.map(g => g.id).toList
        idList should contain(graph1.id)
        idList should contain(graph5.id)
        readyForDeletion.length should be(2)
    }
  }

}
