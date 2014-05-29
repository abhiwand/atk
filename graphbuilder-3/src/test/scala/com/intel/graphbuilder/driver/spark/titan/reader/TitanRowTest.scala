package com.intel.graphbuilder.driver.spark.titan.reader

import org.scalatest.{ Matchers, WordSpec }
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry
import com.thinkaurelius.titan.diskstorage.util.{ StaticByteBuffer, StaticArrayBuffer }

class TitanRowTest extends WordSpec with Matchers {

  "Creating a TitanRow" should {
    "result in a TitanRow instance with rowKey=key1, serializedEntries=Seq(Entry('column','value'))" in {
      val rowKey = new StaticByteBuffer(("key1".getBytes()))
      val columnKey = new StaticArrayBuffer("column".getBytes())
      val columnValue = new StaticArrayBuffer("value".getBytes())

      val entry = StaticBufferEntry.of(columnKey, columnValue)
      val serializedEntries = List(entry)
      val titanRow = new TitanRow(rowKey, serializedEntries)

      titanRow.rowKey shouldBe rowKey
      titanRow.serializedEntries shouldBe (serializedEntries)
    }
  }
}
