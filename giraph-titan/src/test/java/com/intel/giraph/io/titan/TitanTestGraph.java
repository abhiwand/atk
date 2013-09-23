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
package com.intel.giraph.io.titan;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;

import org.apache.log4j.Logger;

public class TitanTestGraph extends StandardTitanGraph {

	/** Class logger. */
	private static final Logger LOG = Logger.getLogger(TitanTestGraph.class);

	public TitanTestGraph(final GraphDatabaseConfiguration configuration) {
		super(configuration);
	}

	public Entry writeEdge(InternalRelation relation, int pos,
			StandardTitanTx tx) {
		return super.edgeSerializer.writeRelation(relation, pos, true, tx);
	}

	@Override
	public void shutdown() {
		super.shutdown();
	}

}
