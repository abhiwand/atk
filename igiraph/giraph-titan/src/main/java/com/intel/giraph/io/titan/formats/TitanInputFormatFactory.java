package com.intel.giraph.io.titan.formats;

import com.intel.graphbuilder.titan.io.GBTitanHBaseInputFormat;
import com.thinkaurelius.titan.hadoop.formats.titan_050.cassandra.CachedTitanCassandraInputFormat;
import com.thinkaurelius.titan.hadoop.formats.titan_050.util.CachedTitanInputFormat;
import org.apache.hadoop.conf.Configuration;

import static com.intel.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN_STORAGE_BACKEND;

/**
 * Create a TitanInputFormat based on the storage backend.
 */
public class TitanInputFormatFactory {

    /**
     * Get the TitanInputFormat based on the storage backend (either HBase or Cassandra)
     */
    public static CachedTitanInputFormat getTitanInputFormat(Configuration conf) {
        CachedTitanInputFormat titanInputFormat;

        if (GIRAPH_TITAN_STORAGE_BACKEND.get(conf).equals("cassandra")) {
            titanInputFormat = new CachedTitanCassandraInputFormat();
        } else {
            titanInputFormat = new GBTitanHBaseInputFormat();
        }

        return (titanInputFormat);
    }
}
