

package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.types.StringType;

public class PropertyGraphElementStringTypeVids extends PropertyGraphElement<StringType> {
    public StringType createVid() {
        return new StringType();
    }
}
