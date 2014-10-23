package com.intel.intelanalytics.domain

import org.scalatest.WordSpec

import DomainJsonProtocol._
import spray.json._
import org.joda.time.{ DateTimeZone, DateTime }

class DomainJsonProtocolTest extends WordSpec {

  "DateTimeFormat" should {
    "be able to serialize" in {
      val dateTime = new DateTime(2000, 3, 13, 5, 49, 22, 888, DateTimeZone.UTC)
      assert(dateTime.toJson.toString == "\"2000-03-13T05:49:22.888Z\"")
    }

    "be able to de-serialize" in {
      val dateTime = new DateTime(2000, 3, 13, 5, 49, 22, 888, DateTimeZone.UTC)
      val json = new JsString("2000-03-13T05:49:22.888Z")
      assert(json.convertTo[DateTime].getMillis == dateTime.getMillis)
    }
  }

}
