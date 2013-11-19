

package com.intel.hadoop.graphbuilder.pipeline.input;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * This interface encapsulates the methods used to configure an end-to-end map reduce chain so
 * that it will work with the input format specified.
 *
 * The first map job corresponds to stepping through the raw input and spitting out property graph elements:
 * The input determines the first mapper.
 *
 * The output determines the first reducer and any subsequent MR tasks in a chain.
 *
 * The methods in this interface are used by the full MR chain to properly configure the first MR job to work
 * with the input mapper.
 *
 */

public interface InputConfiguration {

    public void    updateConfigurationForMapper (Configuration configuration, CommandLine cmd);

    public void    updateJobForMapper(Job job, CommandLine cmd);

    public boolean usesHBase();

    public Class   getMapperClass();

    public String  getDescription();
}
