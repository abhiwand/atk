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

package com.intel.intelanalytics.domain

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import com.intel.intelanalytics.domain.schema._
import org.joda.time.{ DateTime, DateTimeZone }
import org.scalatest.{ Matchers, WordSpec }
import spray.json._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class DomainJsonProtocolTest extends WordSpec with Matchers {

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

    "be able to handle frame schemas" in {
      val schema = new FrameSchema(List(Column("a", DataTypes.int64), Column("b", DataTypes.string)))
      assert(schema.toJson.compactPrint == """{"columns":[{"name":"a","data_type":"int64","index":0},{"name":"b","data_type":"string","index":1}]}""")
    }

    "be able to handle empty frame schemas" in {
      val schema = new FrameSchema(List())
      assert(schema.toJson.compactPrint == """{"columns":[]}""")
    }

    "be able to handle vertex schemas" in {
      val schema = new VertexSchema(List(Column("_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("id", DataTypes.string)), "mylabel", Some("id"))
      assert(schema.toJson.compactPrint == """{"columns":[{"name":"_vid","data_type":"int64","index":0},{"name":"_label","data_type":"string","index":1},{"name":"id","data_type":"string","index":2}],"label":"mylabel","id_column_name":"id"}""")
    }

    "be able to handle edge schemas" in {
      val schema = new EdgeSchema(List(Column("_eid", DataTypes.int64), Column("_src_vid", DataTypes.int64), Column("_dest_vid", DataTypes.int64), Column("_label", DataTypes.string)), "mylabel", "src", "dest", directed = true)
      assert(schema.toJson.compactPrint == """{"columns":[{"name":"_eid","data_type":"int64","index":0},{"name":"_src_vid","data_type":"int64","index":1},{"name":"_dest_vid","data_type":"int64","index":2},{"name":"_label","data_type":"string","index":3}],"label":"mylabel","src_vertex_label":"src","dest_vertex_label":"dest","directed":true}""")
    }

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

    "parse the current format for frame schemas" in {
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
      assert(schema.isInstanceOf[FrameSchema])
    }

    "parse the current format for vertex schemas" in {
      val string = """{"columns":[{"name":"_vid","data_type":"int64","index":0},{"name":"_label","data_type":"string","index":1},{"name":"id","data_type":"string","index":2}],"label":"mylabel","id_column_name":"id"}"""
      val json = JsonParser(string).asJsObject
      val schema = json.convertTo[Schema]
      assert(schema.columnNames.length == 3)
      assert(schema.columnDataType("_label") == DataTypes.string)
      assert(schema.isInstanceOf[VertexSchema])
      val expectedSchema = new VertexSchema(List(Column("_vid", DataTypes.int64), Column("_label", DataTypes.string), Column("id", DataTypes.string)), "mylabel", Some("id"))
      assert(schema == expectedSchema)
    }

    "parse the current format for edge schemas" in {
      val string = """{"columns":[{"name":"_eid","data_type":"int64","index":0},{"name":"_src_vid","data_type":"int64","index":1},{"name":"_dest_vid","data_type":"int64","index":2},{"name":"_label","data_type":"string","index":3}],"label":"mylabel","src_vertex_label":"src","dest_vertex_label":"dest","directed":true}"""
      val json = JsonParser(string).asJsObject
      val schema = json.convertTo[Schema]
      assert(schema.columnNames.length == 4)
      assert(schema.columnDataType("_label") == DataTypes.string)
      assert(schema.isInstanceOf[EdgeSchema])
      val expectedSchema = new EdgeSchema(List(Column("_eid", DataTypes.int64), Column("_src_vid", DataTypes.int64), Column("_dest_vid", DataTypes.int64), Column("_label", DataTypes.string)), "mylabel", "src", "dest", directed = true)
      assert(schema == expectedSchema)
    }
  }

  "javaCollectionFormat" should {
    "parse Java collections to JSON" in {
      val javaSet = Array(1, 2, 3).toSet.asJava
      val javaList = Array("Alice", "Bob", "Charles").toList.asJava

      val jsonSet = javaSet.toJson
      val jsonList = javaList.toJson

      jsonSet.convertTo[java.util.Set[Int]] should contain theSameElementsAs (javaSet)
      jsonList.convertTo[java.util.List[String]] should contain theSameElementsAs (javaList)
    }
  }
  "javaMapFormat" should {
    "parse Java maps to JSON" in {
      val javaHashMap = new java.util.HashMap[String, Int]()
      javaHashMap.put("Alice", 29)
      javaHashMap.put("Bob", 45)
      javaHashMap.put("Jason", 56)

      val jsonMap = javaHashMap.toJson
      val javaJsonToHashMap = jsonMap.convertTo[java.util.HashMap[String, Int]]

      javaJsonToHashMap.keySet() should contain theSameElementsAs (javaHashMap.keySet())
      javaJsonToHashMap.values() should contain theSameElementsAs (javaHashMap.values())
    }
  }
}
