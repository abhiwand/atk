package com.intel.giraph.io.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.log4j.Logger;

import java.io.IOException;

import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.GIRAPH_TITAN;

/**
 * The titan graph writer which connects Giraph to Titan
 * for writing back algorithm results
 */
public class TitanGraphWriter {
    /** LOG class */
    private static final Logger LOG = Logger
            .getLogger(TitanGraphWriter.class);

    /**
     * @param context       task attempt context
     */
    public static TitanGraph open(TaskAttemptContext context) throws IOException {
        TitanGraph graph = null;

        org.apache.commons.configuration.Configuration configuration =
                GiraphToTitanGraphFactory.generateTitanConfiguration(context.getConfiguration(),
                        GIRAPH_TITAN.get(context.getConfiguration()));

        graph = TitanFactory.open(configuration);

        if (null != graph){
            return graph;
        }
        else {
            LOG.fatal("IGIRAPH ERROR: Unable to open titan graph");
            System.exit(-1);
            return null;
        }

    }
}
