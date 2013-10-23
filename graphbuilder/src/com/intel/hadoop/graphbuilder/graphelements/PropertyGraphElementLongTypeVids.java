

package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.types.LongType;

public class PropertyGraphElementLongTypeVids extends PropertyGraphElement<LongType> {
    public LongType createVid() {
        return new LongType();
    }
}