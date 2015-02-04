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

        val readyForDeletion = graphRepo.listReadyForDeletion(age)
        val idList = readyForDeletion.map(g => g.id).toList
        idList should contain(graph1.id)
        println(idList)
        readyForDeletion.length should be(1)
    }
  }

  it should "return a list of graphs ready to have their metadata deleted" in {
    val graphRepo = slickMetaStoreComponent.metaStore.graphRepo
    slickMetaStoreComponent.metaStore.withSession("graph-test") {
      implicit session =>
        val age = 10 * 24 * 60 * 60 * 1000 //10 days

        val name = Some("my_name")

        // create graphs

        //should be in list old and unnamed
        val graph1 = graphRepo.insert(new GraphTemplate(None)).get
        graphRepo.update(graph1.copy(lastReadDate = new DateTime().minus(age * 2), statusId = 8))

        //should not be in list. it is named
        val graph2 = graphRepo.insert(new GraphTemplate(name)).get
        graphRepo.update(graph2.copy(lastReadDate = new DateTime().minus(age * 2), statusId = 8))

        //should not be in list. it is too new
        val graph3 = graphRepo.insert(new GraphTemplate(None)).get
        graphRepo.update(graph3.copy(lastReadDate = new DateTime(), statusId = 8))

        //should be in list wrong status type
        val graph4 = graphRepo.insert(new GraphTemplate(None)).get
        graphRepo.update(graph4.copy(lastReadDate = new DateTime().minus(age * 2), statusId = 4))

        val readyForDeletion = graphRepo.listReadyForMetaDataDeletion(age)
        val idList = readyForDeletion.map(g => g.id).toList
        idList should contain(graph1.id)
        readyForDeletion.length should be(1)
    }
  }

}
