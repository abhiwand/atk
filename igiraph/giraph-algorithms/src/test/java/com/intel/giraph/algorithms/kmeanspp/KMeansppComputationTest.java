package com.intel.giraph.algorithms.kmeanspp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.json.JSONArray;
import org.json.JSONException;
import java.util.Map;
import java.util.Arrays;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.DenseVector;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.io.formats.JsonLongIDDoubleVectorInputFormat;
import com.intel.giraph.io.formats.JsonKMeansppLongVectorOutputFormat;

public class KMeansppComputationTest {

    /** Custom argument for the number of clusters */
    private static final String KMEANSPP_NUM_CENTEROIDS = "kmeanspp.num_centeroids";

    /**
     * Computes the Eucledian distance between two vector.
     *
     * @param truth Ground truth vector.
     * @param calculatedCenteroid Calculated centeroid vector.
     *
     * @return Eucledian distance.
     */
    private double computeCost(Vector truth, Vector calculatedCenteroid) {
        return Math.pow(truth.getDistanceSquared(calculatedCenteroid), 0.5);
    }

    /**
     * Convertes the Double array to primitive double array.
     *
     * @param array Double array.
     *
     * @return Primitive double array.
     */
    private double[] getPrimitiveDoubleArray(Double[] array) {
        double[] primitive = new double[array.length];
        int i = 0;
        for (Double num : array) {
            primitive[i] = (double) array[i];
            i++;
        }
        return primitive;
    }


    /**
     * KMeans++ algorithm test with toy data.
     */
    @Test
    public void testToyData() throws Exception {

        /** Datapoints generated from the following centeroids.
         *
         * centeroid 0: -0.446673  -8.48793  0.378735   0.75713   -7.90668
         * centeroid 1: -4.6875    -1.4584   1.58041    0.741229  -0.535909
         * centeroid 2: -4.4491    -2.82209  9.82591   -0.139542   8.73918
         */
        String[] datapoints = new String[] {
            "[0,[-5.50166,-2.96193,11.4415,0.597497,9.64607]]",
            "[1,[-3.867,-3.34436,10.1229,-1.1747,8.80787]]", 
            "[2,[-3.5601,-2.08241,2.42989,-0.903075,0.627161]]",
            "[3,[-4.26998,-3.63148,9.86694,-1.26365,7.16145]]",
            "[4,[-5.65938,-3.33662,1.11693,1.84786,-1.15744]]",
            "[5,[-5.68864,-0.426649,1.23675,-1.05078,0.913692]]",
            "[6,[-1.41637,-6.95058,-0.242568,2.05659,-7.16376]]",
            "[7,[-0.365944,-7.82902,0.370149,1.26469,-6.39526]]",
            "[8,[0.971999,-7.95588,1.50732,2.25531,-6.12983]]",
            "[9,[-0.512816,-9.08294,1.18597,2.52749,-5.35559]]",
            "[10,[-4.07203,-2.38466,2.93572,1.34905,0.4846]]",
            "[11,[-1.30005,-8.54409,-0.12535,0.872543,-9.09243]]",
            "[12,[-4.47687,-0.446846,3.155,0.981895,0.54858]]",
            "[13,[-4.22296,-1.50292,1.22598,0.782993,1.68536]]",
            "[14,[-4.73848,-2.6958,10.0679,1.13625,9.04625]]",
            "[15,[-0.317596,-6.66834,1.30022,1.02799,-7.54273]]",
            "[16,[-1.83263,-6.86943,-0.596344,1.21669,-7.32347]]",
            "[17,[-5.91463,-0.509836,1.9063,-0.712775,-1.12002]]",
            "[18,[-1.12897,-8.47747,-0.88033,1.64912,-8.69849]]",
            "[19,[0.55175,-9.25044,0.720086,1.08582,-6.69295]]" 
        }; 

        /** Ground truth. */
        double[][] truth = new double[][] {
            {-0.446673, -8.48793, 0.378735, 0.75713, -7.90668},
            {-4.6875, -1.4584, 1.58041, 0.741229, -0.535909},
            {-4.4491, -2.82209, 9.82591, -0.139542, 8.73918}
        };
      
        GiraphConfiguration conf = new GiraphConfiguration(); 

        // configuration
        conf.setComputationClass(KMeansppComputation.class);
        conf.setVertexInputFormatClass(JsonLongIDDoubleVectorInputFormat.class);
        conf.setVertexOutputFormatClass(JsonKMeansppLongVectorOutputFormat.class);
        conf.setMasterComputeClass(KMeansppComputation.KMeansppMasterCompute.class);
        conf.setInt(KMEANSPP_NUM_CENTEROIDS, 3);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, datapoints);

        // parse the results
        Map<Long, Double[]> centeroidMap = parseResults(results);

        /** check the results with the expected results */
        double cost = 0.0;
        for (int idx = datapoints.length; idx < centeroidMap.size() + datapoints.length; idx++) {
            assertTrue(centeroidMap.containsKey(Long.valueOf(idx)));
            double[] centeroid = getPrimitiveDoubleArray(centeroidMap.get(Long.valueOf(idx)));

            Vector centeroidVec = new DenseVector(centeroid);
            if (centeroid[1] < -5) {
                cost += computeCost(new DenseVector(truth[0]), centeroidVec);
            } else if ((-6 < centeroid[4]) && (centeroid[4] < 3)) {
                cost += computeCost(new DenseVector(truth[1]), centeroidVec);
            } else if (centeroid[2] > 6) {
                cost += computeCost(new DenseVector(truth[2]), centeroidVec);
            } else {
                fail("The calculated centeroid is not found from the ground truth.");
            }
        }
        assertTrue((cost/3) < 1.5);
  }

  /**
   * Parse the output.
   *
   * @param results String container of output lines.
   *
   * @return Parsed KV pairs stored in Map.
   */
    private Map<Long, Double[]> parseResults(Iterable<String> results) {
        Map<Long, Double[]> centeroidMap = Maps.newHashMapWithExpectedSize(Iterables.size(results));

        int numResults = 0;
        for (String line : results) {

            // make sure there are only three centeroids
            assertTrue(numResults < 3);

            try {
                JSONArray centeroid = new JSONArray(line);
                if (centeroid.length() != 2) {
                    throw new IllegalArgumentException("Wrong centeroid format.");
                }

                Long centeroidId = centeroid.getLong(0);
                JSONArray vector = centeroid.getJSONArray(1);
                if (vector.length() != 5) {
                    throw new IllegalArgumentException("Wrong centeroid vector format!");
                }

                // make sure key the centeroid IDs are unique
                assertFalse(centeroidMap.containsKey(centeroidId));

                /** parse out centeroid id and centeroid vector */
                Double[] centeroidVector = new Double[5];
                for (int i = 0; i < 5; i++) {
                    centeroidVector[i] = vector.getDouble(i);
                }

                //add the centeroid ID and vector to the map
                centeroidMap.put(centeroidId, centeroidVector);

            } catch (JSONException e) {
                throw new IllegalArgumentException("Failed to obtain centeroid from line " + line, e);
            }
            numResults++;
        }
        return centeroidMap;
    }
}
