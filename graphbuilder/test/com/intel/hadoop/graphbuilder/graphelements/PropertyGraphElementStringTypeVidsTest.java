package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.types.StringType;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class PropertyGraphElementStringTypeVidsTest {
    @Test
    public void testCreateVid() {
        PropertyGraphElementStringTypeVids elt = new PropertyGraphElementStringTypeVids();

        assertNotNull(elt);

        Object vid = elt.createVid();

        assertNotNull(vid);

        assertEquals(vid.getClass(), StringType.class);
    }
}
