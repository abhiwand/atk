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

package com.intel.giraph.io.titan.conf;

import org.apache.giraph.conf.StrConfOption;

/**
 * Constants used all over Giraph for configuration specific for Titan/Hbase
 * Titan/Cassandra
 */

public interface GiraphTitanConstants {

	/** Titan backend type . */
	StrConfOption GIRAPH_TITAN_STORAGE_BACKEND = new StrConfOption(
			"giraph.titan.input.storage.backend", "hbase",
			"Titan backend - required");
	/** Titan/Hbase hostname . */
	StrConfOption GIRAPH_TITAN_STORAGE_HOSTNAME = new StrConfOption(
			"giraph.titan.input.storage.hostname", "localhost",
			"Titan/Hbase hostname - required");
	/** Titan/Hbase table name . */
	StrConfOption GIRAPH_TITAN_STORAGE_TABLENAME = new StrConfOption(
			"giraph.titan.input.storage.tablename", "titan",
			"Titan/Hbase tablename - required");
	/** port where to contact Titan/Hbase. */
	StrConfOption GIRAPH_TITAN_STORAGE_PORT = new StrConfOption(
			"giraph.titan.input.storage.port", "2181",
			"port where to contact Titan/hbase.");
	StrConfOption GIRAPH_TITAN = new StrConfOption("giraph.titan",
			"giraph.titan.input", "Giraph/Titan prefix");
	StrConfOption STORAGE_READ_ONLY = new StrConfOption("storage.read-only",
			"false", "read only or not");
	StrConfOption AUTOTYPE = new StrConfOption("autotype", "none", "autotype");
	StrConfOption VERTEX_PROPERTY_KEY_LIST = new StrConfOption(
			"vertex.property.key.list", "age",
			"the vertex property keys which Giraph needs");
	StrConfOption EDGE_PROPERTY_KEY_LIST = new StrConfOption(
			"edge.property.key.list", "battled",
			"the edge property keys which Giraph needs");
	StrConfOption EDGE_LABEL_LIST = new StrConfOption("edge.label.list",
			"time", "the edge labels which Giraph needs");
}
