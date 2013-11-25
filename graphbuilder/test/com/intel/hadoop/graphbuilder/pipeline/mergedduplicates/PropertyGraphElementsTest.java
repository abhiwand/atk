package com.intel.hadoop.graphbuilder.pipeline.mergedduplicates;


import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.PropertyGraphElement.PropertyGraphElements;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(GraphBuilderExit.class)
public class PropertyGraphElementsTest {
    Iterable<com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement> values;
    PropertyGraphElements graphElements;

    @Before
    public void setUp(){
        com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement elementOne = new Edge();
        com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement elementTwo = new Vertex();

        StringType src = new StringType("src");
        StringType dst = new StringType("dst");
        StringType label = new StringType("label");
        Edge<StringType> edge = new Edge<StringType>(src, dst, label);


        //elementOne.init(com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement.GraphElementType.EDGE, edge);

        StringType vertexId = new StringType("The Greatest Vertex EVER");
        Vertex<StringType> vertex = new Vertex<StringType>(vertexId);
        //elementTwo.init(com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement.GraphElementType.VERTEX, vertex);



        com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement[] elements = {edge, vertex};

        values = Arrays.asList(elements);

        graphElements = new PropertyGraphElements();


    }

    @Test
    public void test(){
        graphElements.mergeDuplicates(values);

    }
}
