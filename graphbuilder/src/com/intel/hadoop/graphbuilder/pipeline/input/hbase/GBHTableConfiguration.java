/**
 * Copyright (C) 2012 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pipeline.input.hbase;

import com.intel.hadoop.graphbuilder.util.RuntimeConfig;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

/**
 * Class holding all static strings
 */
public class GBHTableConfiguration {

    public enum Counters {
        HTABLE_ROWS_READ,
        HTABLE_COLS_READ,
        HTABLE_COLS_IGNORED,
        HTABLE_ROWS_WRITTEN,
        HTABLE_COLS_WRITTEN,
        VERTICES_WRITTEN,
        VERTEX_PROPERTIES_WRITTEN,
        HTABLE_COL_READ_ERROR,
        ERROR,
    };

    // Column name separator cannot be ":" because HBase uses ":" as the
    // separator between column family and qualifier


    public static final String COL_NAME_SEPARATOR = "#";
    public static final String VIDMAP_HTABLE_NAME = "GB_VidMap";
    public static final String VIDMAP_HTABLE_HCD = "vidmap";
    public static final String VCN_CONF_NAME = "VertexColNames";
    public static final String ECN_CONF_NAME = "EdgeColNames";
    public static final String DECN_CONF_NAME = "DirectedEdgeColNames";
    public static final String VERTEX_PROP_COLFAMILY = "VertexPropertyCF";
    public static final String VERTEX_PROP_IDCOLQUALIFIER = "VertexID";

    public static final String HBASE_COLUMN_SEPARATOR           = ":";

    public static final String TRIBECA_GRAPH_PROPERTY_SEPARATOR = "_";
    public static final String NULLKEY                          = "NULLKEY";

    public static RuntimeConfig config = RuntimeConfig.getInstance(GBHTableConfiguration.class);

}
