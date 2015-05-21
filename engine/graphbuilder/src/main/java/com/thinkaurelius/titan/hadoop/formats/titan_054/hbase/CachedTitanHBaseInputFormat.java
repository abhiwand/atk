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

package com.thinkaurelius.titan.hadoop.formats.titan_054.hbase;

import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.formats.hbase.TitanHBaseInputFormat;
import com.thinkaurelius.titan.hadoop.formats.util.TitanInputFormat;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetupCommon;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * An extension of TitanHBaseInputFormat that supports cached Titan connections.
 * <p/>
 * An extension of TitanHBaseInputFormat in Titan 0.5.4 that uses CachedTitanHBaseRecordReader.
 * Some of the class variables in TitanHBaseInputFormat were declared as private so there is
 * some duplication of code.
 *
 * @see com.thinkaurelius.titan.hadoop.formats.hbase.TitanHBaseInputFormat
 */
public class CachedTitanHBaseInputFormat extends TitanHBaseInputFormat {

    private static final Logger log =
            LoggerFactory.getLogger(CachedTitanHBaseInputFormat.class);

    private final TableInputFormat cachedTableInputFormat = new TableInputFormat();
    private byte[] cachedEdgestoreFamily;

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return cachedTableInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new CachedTitanHBaseRecordReader(this,
                vertexQuery,
                (TableRecordReader) cachedTableInputFormat.createRecordReader(inputSplit, taskAttemptContext),
                cachedEdgestoreFamily);
    }

    @Override
    public void setConf(final Configuration config) {
        super.setConf(config);

        if (inputConf.get(HBaseStoreManager.SHORT_CF_NAMES)) {
            cachedEdgestoreFamily = Bytes.toBytes("e");
        } else {
            cachedEdgestoreFamily = Bytes.toBytes(Backend.EDGESTORE_NAME);
        }

        cachedTableInputFormat.setConf(config);
    }

    @Override
    public Configuration getConf() {
        return cachedTableInputFormat.getConf();
    }
}
