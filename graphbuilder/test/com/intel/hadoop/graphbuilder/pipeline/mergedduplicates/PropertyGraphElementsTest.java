package com.intel.hadoop.graphbuilder.pipeline.mergedduplicates;


import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.GraphElements;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.PropertyGraphElements;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;

@RunWith(PowerMockRunner.class)
//@PrepareForTest(GraphBuilderExit.class)
public class PropertyGraphElementsTest {
    Iterable<PropertyGraphElement> values;
    PropertyGraphElements propertyGraphElements;

    @Before
    public void setUp(){
        PropertyGraphElement elementOne = new PropertyGraphElementLongTypeVids();
        PropertyGraphElement elementTwo = new PropertyGraphElementStringTypeVids();

        StringType src = new StringType("src");
        StringType dst = new StringType("dst");
        StringType label = new StringType("label");
        Edge<StringType> edge = new Edge<StringType>(src, dst, label);


        elementOne.init(PropertyGraphElement.GraphElementType.EDGE, edge);

        StringType vertexId = new StringType("The Greatest Vertex EVER");
        Vertex<StringType> vertex = new Vertex<StringType>(vertexId);
        elementTwo.init(PropertyGraphElement.GraphElementType.VERTEX, vertex);



        PropertyGraphElement[] elements = {elementOne, elementTwo};

        values = Arrays.asList(elements);

        propertyGraphElements = new PropertyGraphElements();


    }

    @Test
    public void test(){
       propertyGraphElements.mergeDuplicates(values);

    }
}
