package com.intel.hadoop.graphbuilder.graphelements

import com.intel.hadoop.graphbuilder.types.StringType
import org.junit.Test

import static junit.framework.Assert.assertNotNull
import static junit.framework.Assert.assertNotNull

class PropertyGraphElementStringTypeVidsTest {
    @Test
    void testCreateVid() {
        PropertyGraphElementStringTypeVids elt = new PropertyGraphElementStringTypeVids();

        assertNotNull(elt)

        Object vid = elt.createVid();

        assertNotNull(vid)

        assertEquals(vid.getClass(), StringType.class)

    }
}
