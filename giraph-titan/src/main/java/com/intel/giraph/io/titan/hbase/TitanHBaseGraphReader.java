/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package com.intel.giraph.io.titan.hbase;

import com.google.common.base.Preconditions;
import com.intel.giraph.io.titan.TitanGraphReader;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseConfiguration;

public class TitanHBaseGraphReader extends TitanGraphReader {

	/**
	 * logger
	 */
	private static final Logger LOG = Logger
			.getLogger(TitanHBaseGraphReader.class);

	public TitanHBaseGraphReader(final Configuration configuration) {
		super(configuration);
	}

	public Vertex readGiraphVertexLongDoubleFloat(
			ImmutableClassesGiraphConfiguration conf, byte[] key,
			final NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap) {

		return super.readGiraphVertexLongDoubleFloat(conf,
				ByteBuffer.wrap(key), new HBaseMapIterable(rowMap));
	}

	private static class HBaseMapIterable implements Iterable<Entry> {
		/**
		 * Result from TableRecordReader is in a three level Map of the form:
		 * <Map&family,Map<qualifier,Map<timestamp,value>>>
		 * Map&family is key; Map<qualifier,Map<timestamp,value>> is columnValues
		 **/
		private final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnValues;

		public HBaseMapIterable(
				final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnValues) {
			Preconditions.checkNotNull(columnValues);
			this.columnValues = columnValues;

			for (java.util.Map.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : columnValues
					.entrySet()) {
				NavigableMap<Long, byte[]> cellMap = columnEntry.getValue();

			}
		}

		@Override
		public Iterator<Entry> iterator() {
			return new HBaseMapIterator(columnValues.entrySet().iterator());
		}

	}

	private static class HBaseMapIterator implements Iterator<Entry> {

		private final Iterator<Map.Entry<byte[], NavigableMap<Long, byte[]>>> iterator;

		public HBaseMapIterator(
				final Iterator<Map.Entry<byte[], NavigableMap<Long, byte[]>>> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public Entry next() {
			final Map.Entry<byte[], NavigableMap<Long, byte[]>> entry = iterator
					.next();
			return new StaticBufferEntry(new StaticByteBuffer(entry.getKey()),
					new StaticByteBuffer(entry.getValue().lastEntry()
							.getValue()));
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

}
