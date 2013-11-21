package com.intel.hadoop.graphbuilder.graphelements;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataOutput;
import java.io.IOException;

public abstract class PropertyGraphElement<VidType extends WritableComparable<VidType>> {

    public abstract boolean isEdge();
    public abstract boolean isVertex();
    public abstract void    write(DataOutput output) throws IOException;
}
