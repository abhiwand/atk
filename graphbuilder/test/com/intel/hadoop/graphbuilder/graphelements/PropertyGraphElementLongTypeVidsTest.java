package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.types.LongType;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;


public class PropertyGraphElementLongTypeVidsTest {

    @Test
    public void testCreateVid() {
        SerializedPropertyGraphElementLongTypeVids elt = new SerializedPropertyGraphElementLongTypeVids();

        assertNotNull(elt);

        Object vid = elt.createVid();

        assertNotNull(vid);

        assertEquals(vid.getClass(), LongType.class);

    }
}
