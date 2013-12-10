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
package com.intel.hadoop.graphbuilder.pipeline.output.titan;


import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.graphelements.callbacks.PropertyGraphElementTypeCallback;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;

/**
 *  Add all the edges and vertices into the their respective hashmaps. the edgePropertyTable hash is keyed by edge
 *  id(src, dst, label) with the value being the property map. the  vertexNameToTitanID has is keyed by vertex id
 *  which should be a StringType or LongType with the value bing the titan ID for the vertex.
 *
 * @see LongType
 * @see com.intel.hadoop.graphbuilder.types.StringType
 * @see EdgesIntoTitanReducer
 */
public class EdgesIntoTitanReducerCallback implements PropertyGraphElementTypeCallback{
    private HashMap<EdgeID, Writable> edgePropertyTable;
    private HashMap<Object, Long> vertexNameToTitanID;

    @Override
    public HashMap<EdgeID, Writable> edge(PropertyGraphElement propertyGraphElement, ArgumentBuilder  args) {
        initArguments(args);

        Edge edge   = (Edge) propertyGraphElement;
        EdgeID edgeID = new EdgeID(edge.getSrc(), edge.getDst(), edge.getLabel());

        edgePropertyTable.put(edgeID, edge.getProperties());

        return edgePropertyTable;
    }

    @Override
    public HashMap<Object, Long> vertex(PropertyGraphElement propertyGraphElement, ArgumentBuilder args) {
        initArguments(args);

        Vertex vertex = (Vertex) propertyGraphElement;

        //get the vertex id, StringType or LongType
        Object      vertexId      = vertex.getId();
        PropertyMap propertyMap   = vertex.getProperties();
        //the Titan id we got assigned during the VerticesIntoTitanReducer
        long        vertexTitanId = ((LongType) propertyMap.getProperty("TitanID")).get();

        vertexNameToTitanID.put(vertexId, vertexTitanId);
        return vertexNameToTitanID;
    }

    @Override
    public Object nullElement(PropertyGraphElement propertyGraphElement, ArgumentBuilder args) {
        return null;
    }

    private void initArguments(ArgumentBuilder args){
        if(args.exists("edgePropertyTable") && args.exists("vertexNameToTitanID")){
            edgePropertyTable = (HashMap<EdgeID, Writable>)args.get("edgePropertyTable");
            vertexNameToTitanID = (HashMap<Object, Long>)args.get("vertexNameToTitanID");
        }else{
            throw new IllegalArgumentException("edgePropertyTable and vertexNameToTitanID were not set" );
        }
    }
}
