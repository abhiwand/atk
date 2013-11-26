package com.intel.hadoop.graphbuilder.pipeline.mergedduplicates.propertygraphelement;


import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement.PropertyGraphElements;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.Functional;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.*;

import java.io.IOException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(PropertyGraphElements.class)
public class PropertyGraphElementsTest{

    Iterable<SerializedPropertyGraphElement> values;

    PropertyGraphElements graphElements;
    PropertyGraphElements spiedGraphElements;

    private Configuration configuration;

    private Reducer.Context context;

    private Functional edgeReducerFunction;
    private Functional vertexReducerFunction;

    private TitanGraph graph;

    private SerializedPropertyGraphElement outValue;

    private com.tinkerpop.blueprints.Vertex vertex1;

    org.apache.hadoop.mapreduce.Counter vertexCounter;
    org.apache.hadoop.mapreduce.Counter edgeCounter;
    public static enum Counters {
        NUM_VERTICES,
        NUM_EDGES
    }


    @Before
    public void setUp() throws InstantiationException, IllegalAccessException {
        sampleSerializedGraphElements();

        context = mock(Reducer.Context.class);
        graph = mock(TitanGraph.class);
        configuration = mock(Configuration.class);

        Class valClass = SerializedPropertyGraphElementStringTypeVids.class;
        PowerMockito.when(context.getMapOutputValueClass()).thenReturn(valClass);

        PowerMockito.when(context.getConfiguration()).thenReturn(configuration);

        PowerMockito.when(context.getConfiguration().getBoolean("noBiDir", false)).thenReturn(false);

        mockCounters();

        vertex1 = mock(StandardVertex.class);

        PowerMockito.when(graph.addVertex(null)).thenReturn(vertex1);

        outValue = mock(SerializedPropertyGraphElement.class);

        graphElements = new PropertyGraphElements(edgeReducerFunction, vertexReducerFunction, context, graph,
                outValue, Counters.NUM_EDGES, Counters.NUM_VERTICES );
        spiedGraphElements = spy(graphElements);
    }

    @After
    public void tearDown(){
        values = null;
        edgeReducerFunction = null;
        vertexReducerFunction = null;
        context = null;
        graph = null;
        configuration = null;
        outValue = null;
        graphElements = null;
        tearDownCounters();
    }

    @Test
    public void test_valid_edge_vertex_with_no_edge_or_vertex_reducer_function() throws Exception {
        /*PowerMockito.when(spiedGraphElements,
                method(PropertyGraphElements.class, "getVertexId", com.tinkerpop.blueprints.Vertex.class))
                .withArguments(any(com.tinkerpop.blueprints.Vertex.class)).thenReturn(1L);*/

        spiedGraphElements.mergeDuplicates(values);

        Mockito.verify(vertexCounter).increment(1);
        Mockito.verify(edgeCounter).increment(1);
    }

    @Test
    public void test_valid_edge_vertex_with_broken_edge__reducer_function() throws IOException, InterruptedException {
        //setting up some mocked classes with no stubs should return null property maps
        edgeReducerFunction = mock(Functional.class);
        vertexReducerFunction = mock(Functional.class);

        spiedGraphElements.mergeDuplicates(values);

        Mockito.verify(vertexCounter).increment(1);
        Mockito.verify(edgeCounter).increment(1);
    }

    private void mockCounters(){
        vertexCounter = mock(org.apache.hadoop.mapreduce.Counter.class);
        edgeCounter = mock(org.apache.hadoop.mapreduce.Counter.class);
        PowerMockito.when(context.getCounter(Counters.NUM_VERTICES)).thenReturn(vertexCounter);
        PowerMockito.when(context.getCounter(Counters.NUM_EDGES)).thenReturn(edgeCounter);
    }

    private void tearDownCounters(){
        vertexCounter = null;
        edgeCounter = null;
    }

    /**
     * create our sample Iterable<SerializedPropertyGraphElement> values that gets passed to the reducer
     */
    private void sampleSerializedGraphElements(){
        Edge<StringType> edge = sampleEdge();
        Vertex<StringType> vertex = sampleVertex();

        SerializedPropertyGraphElementStringTypeVids elementOne = new SerializedPropertyGraphElementStringTypeVids();
        elementOne.init(edge);
        SerializedPropertyGraphElementLongTypeVids elementTwo = new SerializedPropertyGraphElementLongTypeVids();
        elementTwo.init(vertex);


        com.intel.hadoop.graphbuilder.graphelements.SerializedPropertyGraphElement[] elements = {elementOne, elementTwo};

        values = Arrays.asList(elements);
    }

    private Edge sampleEdge(){
        StringType src = new StringType("srcVertex");
        StringType dst = new StringType("dstVertex");
        StringType label = new StringType("edgeLabel");
        return new Edge<StringType>(src, dst, label);
    }

    private Vertex sampleVertex(){
        StringType vertexId = new StringType("srcVertex");
        return new Vertex<StringType>(vertexId);
    }
}
