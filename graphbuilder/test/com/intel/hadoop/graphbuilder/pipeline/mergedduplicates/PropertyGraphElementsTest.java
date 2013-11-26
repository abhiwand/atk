package com.intel.hadoop.graphbuilder.pipeline.mergedduplicates;


import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.PropertyGraphElement.PropertyGraphElements;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.DestinationVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.Functional;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.types.system.EmptyVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanKeyVertex;
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex;
import com.tinkerpop.blueprints.TransactionalGraph;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.*;
import static org.powermock.api.support.membermodification.MemberMatcher.method;

import java.io.IOException;
import java.util.*;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;

@RunWith(PowerMockRunner.class)
@PrepareForTest(GraphBuilderExit.class)
public class PropertyGraphElementsTest extends GraphQueryTestSuite  {

    PropertyGraphElementsTest(final GraphTest graphTest) {
        super(graphTest);
    }

    PropertyGraphElementsTest() {
        super();
    }

    Iterable<com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement> values;
    PropertyGraphElements graphElements;

    private Configuration configuration;
    private Reducer.Context context;
    private Functional edgeReducerFunction;
    private Functional vertexReducerFunction;
    private TitanGraph graph;
    private TitanGraph spiedGraph;
    private HashMap<Object, Long>  vertexNameToTitanID;
    private IntWritable outKey;
    private SerializedPropertyGraphElement outValue;
    private Class outClass;

    private final KeyFunction keyFunction = new DestinationVertexKeyFunction();

    private static enum Counters {
        NUM_VERTICES,
        NUM_EDGES
    }



    @Before
    public void setUp() throws InstantiationException, IllegalAccessException {
        com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement elementOne = new Edge();
        com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement elementTwo = new Vertex();

        StringType src = new StringType("src");
        StringType dst = new StringType("dst");
        StringType label = new StringType("label");
        Edge<StringType> edge = new Edge<StringType>(src, dst, label);

        StringType vertexId = new StringType("The Greatest Vertex EVER");
        Vertex<StringType> vertex = new Vertex<StringType>(vertexId);

        com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement[] elements = {edge, vertex};

        values = Arrays.asList(elements);


        edgeReducerFunction = mock(Functional.class);
        vertexReducerFunction = mock(Functional.class);
        context = mock(Reducer.Context.class);
        //graph = mock(TitanGraph.class);
        configuration = mock(Configuration.class);

        Class valClass = SerializedPropertyGraphElementStringTypeVids.class;
        PowerMockito.when(context.getMapOutputValueClass()).thenReturn(valClass);

        PowerMockito.when(context.getConfiguration()).thenReturn(configuration);

        PowerMockito.when(context.getConfiguration().getBoolean("noBiDir", false)).thenReturn(false);

        //graph = (TitanGraph) graphTest.generateGraph();
        spiedGraph = spy(graph);


        //com.tinkerpop.blueprints.Vertex vertex1 =//
        //vertex1.setProperty();

        //PowerMockito.when(graph.addVertex(null)).thenReturn(vertex1);
        //PowerMockito.when(graph.addVertex(null)).thenReturn()
        outValue = mock(SerializedPropertyGraphElement.class);
        graphElements = new PropertyGraphElements(edgeReducerFunction, vertexReducerFunction, context, graph,
                outClass, outValue, Counters.NUM_EDGES, Counters.NUM_VERTICES );


    }

    @Test
    public void test() throws IOException, InterruptedException {
        graphElements.mergeDuplicates(values);

    }


}
