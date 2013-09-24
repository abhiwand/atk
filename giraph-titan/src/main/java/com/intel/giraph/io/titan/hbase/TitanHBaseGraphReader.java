//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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
package com.intel.giraph.io.titan.hbase;

import com.google.common.base.Preconditions;
import com.intel.giraph.io.titan.TitanGraphReader;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.log4j.Logger;

import org.apache.commons.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

/**
 *
 * TitanHBaseGraphReader which helps read in Graph from Titan/HBase
 *
 */
public class TitanHBaseGraphReader extends TitanGraphReader {

    /**
     * logger
     */
    private static final Logger LOG = Logger.getLogger(TitanHBaseGraphReader.class);

    /**
     * TitanHBaseGraphReader Constructor
     *
     * @param configuration to Titan
     */
    public TitanHBaseGraphReader(final Configuration configuration) {
        super(configuration);
    }

    /**
     * readGiraphVertexLongDoubleFloat
     *
     * @param conf : of giraph
     * @param key : key from HBase input data
     * @param columnMap : columnMap from HBase input data, in
     *            Map<qualifier,Map<timestamp,value>> format
     * @return Vertex : Giraph Vertex
     */
    public Vertex readGiraphVertexLongDoubleFloat(ImmutableClassesGiraphConfiguration conf, byte[] key,
            final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap) {

        return super.readGiraphVertexLongDoubleFloat(conf, ByteBuffer.wrap(key), new HBaseMapIterable(
                columnMap));
    }

    /**
     *
     * HBaseMapIterable to create iterator from TableRecordReader Result
     *
     */
    private static class HBaseMapIterable implements Iterable<Entry> {
        /**
         * Result from TableRecordReader is in a three level Map of the form:
         * <Map&family,Map<qualifier,Map<timestamp,value>>> Map&family is key;
         * Map<qualifier,Map<timestamp,value>> is columnMap
         **/
        private final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap;

        /**
         * HBaseMapIterable
         *
         * @param columnMap from HBase
         */
        public HBaseMapIterable(final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap) {
            Preconditions.checkNotNull(columnMap);
            this.columnMap = columnMap;

            for (java.util.Map.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : columnMap.entrySet()) {
                NavigableMap<Long, byte[]> cellMap = columnEntry.getValue();

            }
        }

        /**
         * @return Iterator for HBase columnvalues
         */
        @Override
        public Iterator<Entry> iterator() {
            return new HBaseMapIterator(columnMap.entrySet().iterator());
        }

    }

    /**
     *
     * HBaseMapIterator
     *
     */
    private static class HBaseMapIterator implements Iterator<Entry> {
        /** iterator over columnValues */
        private final Iterator<Map.Entry<byte[], NavigableMap<Long, byte[]>>> iterator;

        /**
         * HBaseMapIterator
         *
         * @param iterator HBase columnValues iterator
         */
        public HBaseMapIterator(final Iterator<Map.Entry<byte[], NavigableMap<Long, byte[]>>> iterator) {
            this.iterator = iterator;
        }

        /**
         * @return boolean whether there is next entry
         */
        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        /**
         * @return StaticBufferEntry next entry
         */
        @Override
        public Entry next() {
            if (iterator.hasNext()) {
                final Map.Entry<byte[], NavigableMap<Long, byte[]>> entry = iterator.next();
                return new StaticBufferEntry(new StaticByteBuffer(entry.getKey()), new StaticByteBuffer(entry
                        .getValue().lastEntry().getValue()));
            } else {
                return null;
            }
        }

        /**
         * remove
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
