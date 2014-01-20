package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import com.intel.hadoop.graphbuilder.test.TestingGraphProvider;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanType;
import com.tinkerpop.blueprints.Vertex;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;

/**
 * Tests for TitanWriterMRChain#initTitanGraph()
 */
public class TitanWriterMRChainInitTitanGraphTest {


    TestingGraphProvider provider = new TestingGraphProvider();
    TitanGraph graph;

    @Before
    public void setup() throws IOException {
        graph = provider.getTitanGraph();
    }

    @After
    public void tearDown()
    {
        provider.cleanUp();
    }

    @Test
    public void testDeclareAndCollectKeys_EmptyCommandLine() throws Exception {

        assertNull(graph.getType(TitanConfig.GB_ID_FOR_TITAN));

        graph.makeKey(TitanConfig.GB_ID_FOR_TITAN).dataType(String.class).indexed(Vertex.class).unique().make();

        Assert.assertNotNull(graph.getType(TitanConfig.GB_ID_FOR_TITAN));

        TitanType type = graph.getType(TitanConfig.GB_ID_FOR_TITAN);
        if (type != null) {
            TitanKey gbIdKey = (TitanKey) type;
            assertNotNull(gbIdKey);
            assertTrue(type.isPropertyKey());
        }

        System.out.println(graph.getType(TitanConfig.GB_ID_FOR_TITAN).getName());




        //String keyCommandLine = "";

        //TitanWriterMRChain chain = new TitanWriterMRChain();
        //HashMap<String, TitanKey> keys = chain.declareAndCollectKeys(graph, keyCommandLine);
        //assertEquals(1, keys.size());

    }

}
