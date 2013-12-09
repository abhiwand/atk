/**
 * Copyright (C) 2012 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */

package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.graphelements.callbacks.*;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
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

    /**
     * all the callback classes we will be using
     */
    private PropertyGraphElementObject propertyGraphElementObject;
    private PropertyGraphElementId propertyGraphElementId;
    private PropertyGraphElementType propertyGraphElementType;
    private PropertyGraphElementDst propertyGraphElementDst;
    private PropertyGraphElementSrc propertyGraphElementSrc;
    private PropertyGraphElementLabel propertyGraphElementLabel;
    private PropertyGraphElementGetProperty propertyGraphElementGetProperty;
    private PropertyGraphElementSetProperties propertyGraphElementSetProperties;

    public PropertyGraphElement(){
        propertyGraphElementObject = new PropertyGraphElementObject();
        propertyGraphElementId = new PropertyGraphElementId();
        propertyGraphElementType = new PropertyGraphElementType();
        propertyGraphElementDst = new PropertyGraphElementDst();
        propertyGraphElementSrc = new PropertyGraphElementSrc();
        propertyGraphElementLabel = new PropertyGraphElementLabel();
        propertyGraphElementGetProperty = new PropertyGraphElementGetProperty();
        propertyGraphElementSetProperties = new PropertyGraphElementSetProperties();
    }

    /**
     * call the edge/vertex/null PropertyGraphElementTypeCallback
     *
     * @see PropertyGraphElementTypeCallback
     *
     * @param propertyGraphElementTypeCallbackCallback any instance of PropertyGraphElementTypeCallback
     * @param args variable length of arguments that might be used by the instance of PropertyGraphElementTypeCallback
     * @param <T> anything that gets returned by the instance of PropertyGraphElementTypeCallback
     * @return anything that gets returned by the instance of PropertyGraphElementTypeCallback
     */
    public  <T> T typeCallback(PropertyGraphElementTypeCallback propertyGraphElementTypeCallbackCallback, ArgumentBuilder args){
        if(this.isEdge()){
            return propertyGraphElementTypeCallbackCallback.edge(this, args);
        }else if(this.isVertex()){
            return propertyGraphElementTypeCallbackCallback.vertex(this, args);
        }else if(this.isNull()){
            return propertyGraphElementTypeCallbackCallback.nullElement(this, args);
        }
        return null;
    }

    public  <T> T typeCallback(PropertyGraphElementTypeCallback propertyGraphElementTypeCallbackCallback){
        ArgumentBuilder args = ArgumentBuilder.newArguments();

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

    public Object getProperty(String key){
        return this.typeCallback(propertyGraphElementGetProperty, ArgumentBuilder.newArguments().with("key", key));
    }

    public void setProperties(PropertyMap propertyMap){
        this.typeCallback(propertyGraphElementSetProperties, ArgumentBuilder.newArguments().
                with("propertyMap", propertyMap));
    }

}


