package com.intel.graphbuilder.driver.spark.titan.reader

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry
import com.thinkaurelius.titan.diskstorage.StaticBuffer

/**
 * Titan's serialized representation of a vertex and its adjacency list in a single row of a key-value store.
 *
 * @param rowKey Serialized row key
 * @param serializedEntries List of serialized entries. Each entry can either be a vertex property or a row.
 */
case class TitanRow(rowKey: StaticBuffer, serializedEntries: Seq[Entry]) {
  require(rowKey != null, "Row key is required")
  require(serializedEntries != null, "Serialized entries are required")
}
