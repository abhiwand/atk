package com.intel.hadoop.graphbuilder.graphelements

import com.intel.hadoop.graphbuilder.types.LongType
import org.junit.Test

import static junit.framework.Assert.assertNotNull


class PropertyGraphElementLongTypeVidsTest {

    @Test
    void testCreateVid() {
        PropertyGraphElementLongTypeVids elt = new PropertyGraphElementLongTypeVids();

        assertNotNull(elt)

        Object vid = elt.createVid();

        assertNotNull(vid)

        assertEquals(vid.getClass(), LongType.class)

    }
}
