package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.graphelements.callbacks.*;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataOutput;
import java.io.IOException;

/**
 * base the vertex/edge graph element.
 * @param <VidType>
 */
public abstract class PropertyGraphElement<VidType extends WritableComparable<VidType>> {

    public abstract boolean isEdge();
    public abstract boolean isVertex();
    public abstract boolean isNull();
    public abstract String toString();
    public abstract PropertyMap getProperties();
    public abstract void    write(DataOutput output) throws IOException;

    private PropertyGraphElementObject propertyGraphElementObject;
    private PropertyGraphElementId propertyGraphElementId;
    private PropertyGraphElementType propertyGraphElementType;
    private PropertyGraphElementDst propertyGraphElementDst;
    private PropertyGraphElementSrc propertyGraphElementSrc;
    private PropertyGraphElementLabel propertyGraphElementLabel;

    public PropertyGraphElement(){
        propertyGraphElementObject = new PropertyGraphElementObject();
        propertyGraphElementId = new PropertyGraphElementId();
        propertyGraphElementType = new PropertyGraphElementType();
        propertyGraphElementDst = new PropertyGraphElementDst();
        propertyGraphElementSrc = new PropertyGraphElementSrc();
        propertyGraphElementLabel = new PropertyGraphElementLabel();
    }

    public  <T> T typeCallback(PropertyGraphElementTypeCallback propertyGraphElementTypeCallbackCallback, Object ... args){
        if(this.isEdge()){
            return propertyGraphElementTypeCallbackCallback.edge(this, args);
        }else if(this.isVertex()){
            return propertyGraphElementTypeCallbackCallback.vertex(this, args);
        }else if(this.isNull()){
            return propertyGraphElementTypeCallbackCallback.nullElement(this, args);
        }
        return null;
    }

    public <T extends PropertyGraphElement> T get(){
        return this.typeCallback(propertyGraphElementObject);
    }

    public Object getId(){
        return this.typeCallback(propertyGraphElementId);
    }

    public Enum getType(){
        return this.typeCallback(propertyGraphElementType);
    }

    public Object getDst(){
        return this.typeCallback(propertyGraphElementDst);
    }

    public Object getSrc(){
        return this.typeCallback(propertyGraphElementSrc);
    }

    public Object getLabel(){
        return this.typeCallback(propertyGraphElementLabel);
    }


}


