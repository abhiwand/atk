/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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

        if ("cassandra".equals(GIRAPH_TITAN_STORAGE_BACKEND.get(conf))) {
            titanInputFormat = new CachedTitanCassandraInputFormat();
        } else {
            titanInputFormat = new GBTitanHBaseInputFormat();
        }

        return (titanInputFormat);
    }
}
