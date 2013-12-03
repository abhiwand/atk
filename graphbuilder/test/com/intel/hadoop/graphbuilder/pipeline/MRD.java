package com.intel.hadoop.graphbuilder.pipeline;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mrunit.internal.mapreduce.ContextDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.mrunit.internal.util.ArgumentChecker.returnNonNull;

/**
 * Created with IntelliJ IDEA.
 * User: rodorad
 * Date: 12/2/13
 * Time: 6:46 PM
 * To change this template use File | Settings | File Templates.
 */

/*
public class ReduceDriver<K1, V1, K2, V2> extends
    ReduceDriverBase<K1, V1, K2, V2, ReduceDriver<K1, V1, K2, V2>> implements
    ContextDriver {
 */
public class MRD <K1, V1, K2, V2, K3, V3> extends
        MapReduceDriver<K1, V1, K2, V2, K3, V3> implements ContextDriver {

    public static final Log LOG = LogFactory.getLog(MRD.class);

    private MapDriver<K1, V1, K2, V2> myMapDriver;
    private ReduceDriver<K2, V2, K3, V3> myReduceDriver;

    @SuppressWarnings("rawtypes")
    private Class<? extends OutputFormat> outputFormatClass;
    @SuppressWarnings("rawtypes")
    private Class<? extends InputFormat> inputFormatClass;

    MRD(MapDriver<K1, V1, K2, V2> mapper,ReduceDriver<K2, V2, K3, V3> reducer){
        super.setCounters(new Counters());
        setMyMapDriver(mapper);
        setMyReduceDriver(reducer);
    }

    public static <K1, V1, K2, V2, K3, V3> MapReduceDriver<K1, V1, K2, V2, K3, V3> newMapReduceDriver(
            final MapDriver<K1, V1, K2, V2> mapper, final ReduceDriver<K2, V2, K3, V3> reducer) {
        return new MRD<K1, V1, K2, V2, K3, V3>(mapper, reducer);
    }

    @Override
    public List<Pair<K3, V3>> run() throws IOException {
        MapDriver<K1, V1, K2, V2> myMapDriver = getMyMapDriver();
        ReduceDriver<K2, V2, K3, V3> myReduceDriver = getMyReduceDriver();
        try {
            preRunChecks(myMapDriver, getMyReduceDriver());
            initDistributedCache();
            List<Pair<K2, V2>> mapOutputs = new ArrayList<Pair<K2, V2>>();
            // run map component
            LOG.debug("Starting map phase with mapper: " + myMapDriver);
            mapOutputs.addAll(myMapDriver
                    .withCounters(getCounters()).withConfiguration(getConfiguration())
                    .withAll(inputList).withMapInputPath(getMapInputPath()).run());
            /*if (myCombiner != null) {
                // User has specified a combiner. Run this and replace the mapper outputs
                // with the result of the combiner.
                LOG.debug("Starting combine phase with combiner: " + myCombiner);
                mapOutputs = new ReducePhaseRunner<K2, V2>().runReduce(
                        shuffle(mapOutputs), myCombiner);
            }*/
            // Run the reduce phase.
            LOG.debug("Starting reduce phase with reducer: " + myReduceDriver);
            return new ReducePhaseRunner<K3, V3>().runReduce(shuffle(mapOutputs),
                    myReduceDriver);
        } finally {
            cleanupDistributedCache();
        }
    }

    /**
     * The private class to manage starting the reduce phase is used for type
     * genericity reasons. This class is used in the run() method.
     */
    private class ReducePhaseRunner<OUTKEY, OUTVAL> {
        private List<Pair<OUTKEY, OUTVAL>> runReduce(
                final List<Pair<K2, List<V2>>> inputs,
                final ReduceDriver<K2, V2, OUTKEY, OUTVAL> reduceDriver1) throws IOException {

            final List<Pair<OUTKEY, OUTVAL>> reduceOutputs = new ArrayList<Pair<OUTKEY, OUTVAL>>();

            if (!inputs.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    final StringBuilder sb = new StringBuilder();
                    for (Pair<K2, List<V2>> input : inputs) {
                        formatValueList(input.getSecond(), sb);
                        LOG.debug("Reducing input (" + input.getFirst() + ", " + sb + ")");
                        sb.delete(0, sb.length());
                    }
                }

                /*final ReduceDriver<K2, V2, OUTKEY, OUTVAL> myReduceDriver = ReduceDriver*/
                reduceDriver1
                        .withCounters(getCounters())
                        .withConfiguration(getConfiguration()).withAll(inputs);

                if (getOutputSerializationConfiguration() != null) {
                    reduceDriver1
                            .withOutputSerializationConfiguration(getOutputSerializationConfiguration());
                }

                if (outputFormatClass != null) {
                    reduceDriver1.withOutputFormat(outputFormatClass, inputFormatClass);
                }

                reduceOutputs.addAll(reduceDriver1.run());
            }

            return reduceOutputs;
        }
    }

    public MapDriver<K1, V1, K2, V2> getMyMapDriver() {
        return myMapDriver;
    }

    public ReduceDriver<K2, V2, K3, V3> getMyReduceDriver() {
        return myReduceDriver;
    }

    public void setMyMapDriver(MapDriver<K1, V1, K2, V2> myMapDriver) {
        this.myMapDriver = myMapDriver;
    }

    public void setMyReduceDriver(ReduceDriver<K2, V2, K3, V3> myReduceDriver) {
        this.myReduceDriver = myReduceDriver;
    }
}
