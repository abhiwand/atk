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
package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.propertygraphelement;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.callbacks.PropertyGraphElementTypeCallback;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import com.intel.hadoop.graphbuilder.util.Functional;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;

/**
 * <p>
 * Remove any duplicate edges and vertices. If duplicates are found either merge their property maps or call an
 * optional Edge/Vertex reducer
 * </p>
 * <p>
 * This will be called on the property graph element as we are iterating through the list received by the reducer.
 * </p>
 *
 * <p>
 * all arguments are extracted from the argument builder and all are mandatory except the edgeReducerFunction,
 * vertexReducerFunction.
 * <ul>
 *      <li>edgeSet - hashmap with the current list of merged edges</li>
 *      <li>vertexSet - hashmap with the current list of merged vertices</li>
 *      <li>edgeReducerFunction - optional edge reducer function</li>
 *      <li>vertexReducerFunction - optional vertex reducer function</li>
 *      <li>vertexLabelMap - list of vertex labels to be used for writing rdf output</li>
 *      <li>noBiDir - are we cleaning bidirectional edges. if true then remove bidirectional edge</li>
 * </ul>
 * </p>
 *
 * @see PropertyGraphElements
 * @see ContainsKey
 */
public class PropertyGraphElementPut implements PropertyGraphElementTypeCallback {

    private HashMap<EdgeID, Writable> edgeSet;
    private HashMap<Object, Writable>   vertexSet;
    private HashMap<Object, StringType>    vertexLabelMap;

    private Functional edgeReducerFunction;
    private Functional vertexReducerFunction;

    private boolean noBiDir = false;

    private ContainsKey containsKey;

    PropertyGraphElementPut(){
        containsKey = new ContainsKey();
    }

    /**
     *
     * @param propertyGraphElement the property graph element we will check for duplicates
     * @param args list of arguments
     * @return the updated edge set
     */
    @Override
    public HashMap<EdgeID, Writable> edge(PropertyGraphElement propertyGraphElement, ArgumentBuilder args) {
        this.arguments(args);

        EdgeID edgeID = (EdgeID)propertyGraphElement.getId();

        if (((Edge)propertyGraphElement).isSelfEdge()) {
            // self edges are omitted
            return null;
        }

        if(containsKey(propertyGraphElement)){
            // edge is a duplicate

            if (edgeReducerFunction != null) {
                edgeSet.put(edgeID, edgeReducerFunction.reduce(propertyGraphElement.getProperties(), edgeSet.get(edgeID)));
            } else {

                /**
                 * default behavior is to merge the property maps of duplicate edges
                 * conflicting key/value pairs get overwritten
                 */

                PropertyMap existingPropertyMap = (PropertyMap) edgeSet.get(edgeID);
                existingPropertyMap.mergeProperties(propertyGraphElement.getProperties());
            }

        }else{
            if (noBiDir && edgeSet.containsKey(edgeID.reverseEdge())) {
                // in this case, skip the bi-directional edge
            } else {
                // edge is either not bi-directional, or we are keeping bi-directional edges
                if (edgeReducerFunction != null) {
                    edgeSet.put(edgeID, edgeReducerFunction.reduce(propertyGraphElement.getProperties(),edgeReducerFunction.identityValue()));
                } else {
                    edgeSet.put(edgeID, propertyGraphElement.getProperties());
                }
            }
        }
        return edgeSet;
    }

    /**
     *
     * @param propertyGraphElement the property graph element we will check for duplicates
     * @param args see the arguments method for the expected argument list
     * @return updated vertex set
     */
    @Override
    public HashMap<Object, Writable>  vertex(PropertyGraphElement propertyGraphElement, ArgumentBuilder args) {
        this.arguments(args);

        Object vid = propertyGraphElement.getId();

        // track the RDF labels of vertices
        if (propertyGraphElement.getLabel() != null) {
            if (!vertexLabelMap.containsKey(propertyGraphElement.getId())) {
                vertexLabelMap.put(propertyGraphElement.getId(), (StringType)propertyGraphElement.getLabel());
            }
        }

        if(containsKey(propertyGraphElement)){
            if (vertexReducerFunction != null) {
                vertexSet.put(vid,
                        vertexReducerFunction.reduce(propertyGraphElement.getProperties(),
                                vertexSet.get(vid)));
            } else {

                /**
                 * default behavior is to merge the property maps of duplicate vertices
                 * conflicting key/value pairs get overwritten
                 */

                PropertyMap existingPropertyMap = (PropertyMap) vertexSet.get(vid);
                existingPropertyMap.mergeProperties(propertyGraphElement.getProperties());
            }

        }else{
            if (vertexReducerFunction != null) {
                vertexSet.put(vid, vertexReducerFunction.reduce(
                        propertyGraphElement.getProperties(),vertexReducerFunction.identityValue()));
            } else {
                vertexSet.put(vid, propertyGraphElement.getProperties());
            }
        }
        return null;
    }

    @Override
    public <T> T nullElement(PropertyGraphElement propertyGraphElement, ArgumentBuilder args) {
        return null;
    }

    /**
     * Gets all our arguments from the argument builder.
     * <ul>
     *      <li>edgeSet - hashmap with the current list of merged edges</li>
     *      <li>vertexSet - hashmap with the current list of merged vertices</li>
     *      <li>edgeReducerFunction - optional edge reducer function</li>
     *      <li>vertexReducerFunction - optional vertex reducer function</li>
     *      <li>vertexLabelMap - list of vertex labels to be used for writing rdf output</li>
     *      <li>noBiDir - are we cleaning bidirectional edges. if true then remove bidirectional edge</li>
     * </ul>
     * @param args an ArgumentBuilder with all the necessary arguments
     *
     * @see Functional
     */
    private void arguments(ArgumentBuilder args){
        edgeSet = (HashMap<EdgeID, Writable>)args.get("edgeSet");
        vertexSet = (HashMap<Object, Writable>)args.get("vertexSet");
        edgeReducerFunction = (Functional)args.get("edgeReducerFunction", null);
        vertexReducerFunction = (Functional)args.get("vertexReducerFunction", null);
        vertexLabelMap = (HashMap<Object, StringType>)args.get("vertexLabelMap");
        noBiDir = (boolean)args.get("noBiDir");
    }


    /**
     * Runs a PropertyGraphElementTypeCallback to see if the element exist in either the edge or vertex set.
     *
     * @see ContainsKey
     * @see PropertyGraphElementTypeCallback
     * @param propertyGraphElement element to check for
     * @return weather or not the property graph elements key exist in either the vertex or edge hash set.
     */
    private boolean containsKey(PropertyGraphElement propertyGraphElement){
        return (Boolean)propertyGraphElement.typeCallback(containsKey, ArgumentBuilder.newArguments()
                .with("edgeSet", edgeSet).with("vertexSet", vertexSet));
    }
}
