package com.intel.hadoop.graphbuilder.pipeline.mergedduplicates;


import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementLongTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.GraphElements;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(GraphBuilderExit.class)
public class GraphElementsTest {
    Iterable<PropertyGraphElement> values;
    GraphElements graphElements;

    @Before
    public void setUp(){
        PropertyGraphElement elementOne = new PropertyGraphElementLongTypeVids();
        PropertyGraphElement elementTwo = new PropertyGraphElementStringTypeVids();

        PropertyGraphElement[] elements = {elementOne, elementTwo};

        values = Arrays.asList(elements);

        graphElements = new GraphElements();


    }

    @Test
    public void test(){
        graphElements.mergeDuplicates(values);

    }
}
