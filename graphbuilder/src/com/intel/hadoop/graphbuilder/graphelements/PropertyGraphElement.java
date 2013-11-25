package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.graphelements.callbacks.PropertyGraphElementId;
import com.intel.hadoop.graphbuilder.graphelements.callbacks.PropertyGraphElementObject;
import com.intel.hadoop.graphbuilder.graphelements.callbacks.PropertyGraphElementType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataOutput;
import java.io.IOException;

public abstract class PropertyGraphElement<VidType extends WritableComparable<VidType>> {

    public abstract boolean isEdge();
    public abstract boolean isVertex();
    public abstract boolean isNull();
    public abstract String toString();
    public abstract PropertyMap getProperties();
    public abstract void    write(DataOutput output) throws IOException;

    private PropertyGraphElementObject propertyGraphElementObject;
    private PropertyGraphElementId propertyGraphElementId;

    public PropertyGraphElement(){
        propertyGraphElementObject = new PropertyGraphElementObject();
        propertyGraphElementId = new PropertyGraphElementId();
    }

    public  <T> T typeCallback(PropertyGraphElementType propertyGraphElementTypeCallback, Object ... args){
        if(this.isEdge()){
            return propertyGraphElementTypeCallback.edge(this, args);
        }else if(this.isVertex()){
            return propertyGraphElementTypeCallback.vertex(this, args);
        }else if(this.isNull()){
            return propertyGraphElementTypeCallback.nullElement(this, args);
        }
        return null;
    }

    public <T extends PropertyGraphElement> T get(){
        return this.typeCallback(propertyGraphElementObject);
    }

    public Object getId(){
        return this.typeCallback(propertyGraphElementId);
    }
}


