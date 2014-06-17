package com.intel.intelanalytics.service.v1.viewmodels

import org.scalatest.{ Matchers, FlatSpec }

class RelLinkSpec extends FlatSpec with Matchers {

  "RelLink" should "be able to create a self link" in {

    val uri = "http://www.example.com/"
    val relLink = Rel.self(uri)

    relLink.rel should be("self")
    relLink.method should be("GET")
    relLink.uri should be(uri)
  }
}
