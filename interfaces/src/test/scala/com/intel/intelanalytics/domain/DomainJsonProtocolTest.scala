package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema }
import org.joda.time.{ DateTime, DateTimeZone }
import org.scalatest.WordSpec
import spray.json._

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

  "SchemaConversionFormat" should {

    "parse legacy format" in {
      val string =
        """
          |{
          |   "columns": [
          |              ["foo", "str"]
          |   ]
          |}
        """.stripMargin
      val json = JsonParser(string).asJsObject
      val schema = json.convertTo[Schema]
      assert(schema.columnNames.length == 1)
      assert(schema.columnDataType("foo") == DataTypes.string)
    }

    "parse the current format" in {
      val string =
        """
          |{
          |   "columns": [
          |          {"name": "foo", "data_type": "str", "index": -1 }
          |   ]
          |}
        """.
          stripMargin
      val json = JsonParser(string).asJsObject
      val schema = json.convertTo[Schema]
      assert(schema.columnNames.length == 1)
      assert(schema.columnDataType("foo") == DataTypes.string)
    }
  }
}
