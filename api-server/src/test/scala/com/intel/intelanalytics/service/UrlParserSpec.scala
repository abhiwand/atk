package com.intel.intelanalytics.service

import org.scalatest.{ Matchers, FlatSpec }

class UrlParserSpec extends FlatSpec with Matchers {

  "UrlParser" should "be able to parse graphIds from graph URI's" in {
    val uri = "http://example.com/v1/graphs/34"
    UrlParser.getGraphId(uri) should be(Some(34))
  }

  it should "be able to parse frameIds from frame URI's" in {
    val uri = "http://example.com/v1/dataframes/55"
    UrlParser.getFrameId(uri) should be(Some(55))
  }

  it should "NOT parse frameIds from invalid URI's" in {
    val uri = "http://example.com/v1/invalid/55"
    UrlParser.getFrameId(uri) should be(None)
  }

  it should "NOT parse non-numeric frame ids" in {
    val uri = "http://example.com/v1/invalid/ABC"
    UrlParser.getFrameId(uri) should be(None)
  }
}
