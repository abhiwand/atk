package com.intel.intelanalytics.repository

import org.scalatest.Matchers
import com.intel.intelanalytics.domain.graph.GraphTemplate


class GraphRepositorySpec extends SlickMetaStoreH2Testing with Matchers {

   "GraphRepository" should "be able to create graphs" in {
     val graphRepo = slickMetaStoreComponent.metaStore.graphRepo
     slickMetaStoreComponent.metaStore.withSession("graph-test") {
       implicit session =>

         val name = "my-name"

         // create a graph
         val graph = graphRepo.insert(new GraphTemplate(name))
         graph.get should not be null

         // look it up and validate expected values
         val graph2 = graphRepo.lookup(graph.get.id)
         graph2.get should not be null
         graph2.get.name shouldBe name
         graph2.get.createdOn should not be null
         graph2.get.modifiedOn should not be null

          //lookup graph with name and validate expected values
          val graph3 = graphRepo.lookupByName(graph.get.name)
          graph3.get should not be null
          graph3.get.name shouldBe name
          graph3.get.createdOn should not be null
          graph3.get.modifiedOn should not be null
     }
   }

 }
