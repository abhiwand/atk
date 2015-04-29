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

package com.thinkaurelius.titan.hadoop.formats.titan_050.util;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.FaunusVertexQueryFilter;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.PIPELINE_TRACK_PATHS;

/**
 * A temporary fix for Issue#817 KCVSLog$MessagePuller does not shut down when using the TitanInputFormat
 *
 * The Spark context does no shut down due to a runaway KCVSLog$MessagePuller thread that maintained a
 * connection to the underlying graph. Affects Titan 0.5.0 and 0.5.1. This code should be replaced once
 * Titan checks in a fix.
 *
 * @link https://github.com/thinkaurelius/titan/issues/817
 */
public abstract class CachedTitanInputFormat extends InputFormat<NullWritable, FaunusVertex> implements Configurable {

    public static final String SETUP_PACKAGE_PREFIX = "com.thinkaurelius.titan.hadoop.formats.util.input.";
    public static final String SETUP_CLASS_NAME = ".TitanHadoopSetupImpl";

    protected FaunusVertexQueryFilter vertexQuery;
    protected boolean trackPaths;
    //protected TitanHadoopSetup titanSetup;
    protected ModifiableHadoopConfiguration faunusConf;
    protected ModifiableConfiguration titanInputConf;

    @Override
    public void setConf(final Configuration config) {

        this.vertexQuery = FaunusVertexQueryFilter.create(config);

        this.faunusConf = ModifiableHadoopConfiguration.of(config);
        this.titanInputConf = faunusConf.getInputConf();
        //final String titanVersion = faunusConf.get(TITAN_INPUT_VERSION);
        this.trackPaths = faunusConf.get(PIPELINE_TRACK_PATHS);
        //final String className = SETUP_PACKAGE_PREFIX + titanVersion + SETUP_CLASS_NAME;

        //this.titanSetup = ConfigurationUtil.instantiate(className, new Object[]{config}, new Class[]{Configuration.class});
    }
}
