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
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataOutput;
import java.io.IOException;

/**
 * base for the vertex/edge graph element.
 * @param <VidType>
 * @code
 */
public abstract class PropertyGraphElement<VidType extends WritableComparable<VidType>> {

    public abstract boolean isEdge();
    public abstract boolean isVertex();
    public abstract boolean isNull();
    public abstract String toString();
    public abstract PropertyMap getProperties();
    public abstract Object getProperty(String key);
    public abstract void    write(DataOutput output) throws IOException;
    public abstract StringType getLabel();
    public abstract Object getId();
    public abstract PropertyGraphElement get();

    /**
     * all the callback classes we will be using
     */
    private PropertyGraphElementType propertyGraphElementType;
    private PropertyGraphElementDst propertyGraphElementDst;
    private PropertyGraphElementSrc propertyGraphElementSrc;

    public PropertyGraphElement(){
        propertyGraphElementType = new PropertyGraphElementType();
        propertyGraphElementDst = new PropertyGraphElementDst();
        propertyGraphElementSrc = new PropertyGraphElementSrc();
    }

    /**
     * call the edge/vertex/null PropertyGraphElementTypeCallback
     *
     * @see PropertyGraphElementTypeCallback
     *
     * @param propertyGraphElementTypeCallback any instance of PropertyGraphElementTypeCallback
     * @param args variable length of arguments that might be used by the instance of PropertyGraphElementTypeCallback
     * @param <T> anything that gets returned by the instance of PropertyGraphElementTypeCallback
     * @return anything that gets returned by the instance of PropertyGraphElementTypeCallback
     */
    public  <T> T typeCallback(PropertyGraphElementTypeCallback propertyGraphElementTypeCallback, ArgumentBuilder args){
        if(this.isEdge()){
            return propertyGraphElementTypeCallback.edge(this, args);
        }else if(this.isVertex()){
            return propertyGraphElementTypeCallback.vertex(this, args);
        }
        return null;
    }

    public  <T> T typeCallback(PropertyGraphElementTypeCallback propertyGraphElementTypeCallback){
        ArgumentBuilder args = ArgumentBuilder.newArguments();

        if(this.isEdge()){
            return propertyGraphElementTypeCallback.edge(this, args);
        }else if(this.isVertex()){
            return propertyGraphElementTypeCallback.vertex(this, args);
        }
        return null;
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
}


