package com.intel.giraph.algorithms.lbp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.intel.giraph.io.formats.JsonLongIDTwoVectorValueOutputFormat;
import com.intel.giraph.io.formats.JsonLongTwoVectorDoubleTwoVectorInputFormat;


public class LoopyBeliefPropagationComputationTest {

  /**
   * A local test on toy data
   */
  @Test
  public void testToyData() throws Exception {
    // a small four vertex graph
    String[] graph = new String[] {
        "[0,[1,0.1,0.1],[[1,1],[3,3]]]",
        "[1,[0.2,2,2],[[0,1],[2,2],[3,1]]]",
        "[2,[0.3,0.3,3],[[1,2],[4,4]]]",
        "[3,[0.4,4,0.4],[[0,3],[1,1],[4,4]]]",
        "[4,[5,5,0.5],[[3,4],[2,4]]]"
    };

    GiraphConfiguration conf = new GiraphConfiguration();

    conf.setComputationClass(LoopyBeliefPropagationComputation.class);
    conf.setVertexInputFormatClass(
        JsonLongTwoVectorDoubleTwoVectorInputFormat.class);
    conf.setVertexOutputFormatClass(
        JsonLongIDTwoVectorValueOutputFormat.class);
    conf.set("lbp.maxSupersteps", "5");

    // run internally
    Iterable<String> results = InternalVertexRunner.run(conf, graph);

    Map<Long, Double[]> vertexValues = parseVertexValues(results);

    // verify results
    assertNotNull(vertexValues);
    assertEquals(5, vertexValues.size());
    for (long i = 0; i < 5; i++) {
      assertEquals(3, vertexValues.get(1L).length);
      assertEquals(1.0, vertexValues.get(i)[1], 0.05d);    
    }
  }
  
  private Map<Long, Double[]> parseVertexValues(Iterable<String> results) {
    Map<Long, Double[]> vertexValues =
        Maps.newHashMapWithExpectedSize(Iterables.size(results));
    for (String line : results) {
      try {
        JSONArray jsonVertex = new JSONArray(line);
        if (jsonVertex.length() != 2) {
          System.err.println("Wrong vertex output format!");
          System.exit(-1);
        }
        long id = jsonVertex.getLong(0);
        JSONArray valueArray = jsonVertex.getJSONArray(1);
        if (valueArray.length() != 3) {
          System.err.println("Wrong vertex output value format!");
          System.exit(-1);
        }
        Double[] values = new Double[3];
        for (int i = 0; i < 3; i++) {
          values[i] = valueArray.getDouble(i);
        }
        vertexValues.put(id, values);
      } catch (JSONException e) {
        throw new IllegalArgumentException(
            "Couldn't get vertex from line " + line, e);
      }
    }
    return vertexValues;
  }
}
