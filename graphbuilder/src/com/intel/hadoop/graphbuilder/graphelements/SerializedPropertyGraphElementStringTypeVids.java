

package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.types.StringType;

public class SerializedPropertyGraphElementStringTypeVids extends SerializedPropertyGraphElement<StringType> {
    public StringType createVid() {
        return new StringType();
    }
}
