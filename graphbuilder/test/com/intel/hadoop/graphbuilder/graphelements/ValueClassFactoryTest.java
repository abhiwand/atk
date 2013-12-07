package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class ValueClassFactoryTest {
    @Test
    public void testGetValueClassByVidClassName() throws Exception {

        assertSame(ValueClassFactory.getValueClassByVidClassName(StringType.class.getName()),
                SerializedPropertyGraphElementStringTypeVids.class);

        assertSame(ValueClassFactory.getValueClassByVidClassName(LongType.class.getName()),
                SerializedPropertyGraphElementLongTypeVids.class);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testIllegalArgumentException() {
        // String is not an allowed class because it's not a Writable...
        ValueClassFactory.getValueClassByVidClassName(String.class.getName());
    }

    @Test public void testConstructor() {

        // nothing to do here yet, really a placeholder
        ValueClassFactory vcf = new ValueClassFactory();
        assertNotNull(vcf);
    }
}
