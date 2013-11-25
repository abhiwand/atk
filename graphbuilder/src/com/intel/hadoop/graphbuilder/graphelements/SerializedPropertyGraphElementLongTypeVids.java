

package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.types.LongType;

public class SerializedPropertyGraphElementLongTypeVids extends SerializedPropertyGraphElement<LongType> {
    public LongType createVid() {
        return new LongType();
    }
}