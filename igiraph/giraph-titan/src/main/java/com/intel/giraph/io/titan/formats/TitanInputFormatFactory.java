package com.intel.giraph.io.titan.formats;

import com.intel.graphbuilder.io.GBTitanHBaseInputFormat;
import com.intel.graphbuilder.io.titan.formats.cassandra.TitanCassandraInputFormat;
import com.intel.graphbuilder.io.titan.formats.util.TitanInputFormat;
import org.apache.hadoop.conf.Configuration;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_BACKEND;

/**
 * Create a TitanInputFormat based on the storage backend.
 */
public class TitanInputFormatFactory {

    /**
     * Get the TitanInputFormat based on the storage backend (either HBase or Cassandra)
     */
    public static TitanInputFormat getTitanInputFormat(Configuration conf) {
        TitanInputFormat titanInputFormat;

        if (GIRAPH_TITAN_STORAGE_BACKEND.get(conf).equals("cassandra")) {
            titanInputFormat = new TitanCassandraInputFormat();
        } else {
            titanInputFormat = new GBTitanHBaseInputFormat();
        }

        return (titanInputFormat);
    }
}
