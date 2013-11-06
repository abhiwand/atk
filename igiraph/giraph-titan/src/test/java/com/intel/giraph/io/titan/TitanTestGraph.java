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
        LOG.info("create TitanTestGraph");
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

}
