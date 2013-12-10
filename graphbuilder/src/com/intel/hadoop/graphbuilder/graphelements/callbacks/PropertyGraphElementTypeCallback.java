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
package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;

/**
 * Very simple interface that gets called when the property graph element is an edge, vertex or null graph element.
 * This essentially allows us to do a callback based on graph element type and it centralizes the branching on type to a
 * single function to PropertyGraphElement.typeCallback(). Makes the PropertyGraphElement much nicer because you don't
 * have to worry about weather your vertex or an edge has much.
 *
 * <P><b>With interface</b><br />
 * SerializedPropertyGraphElement.graphElement().getType()
 * <b>without interface</b>
 * ((Edge)SerializedPropertyGraphElement.graphElement()).getType()
 * </p>
 *
 * <p>The interface gets called from PropertyGraphElement.typeCallback</p>
 * <pre>
 * <code>
 *     public  <T> T typeCallback(PropertyGraphElementTypeCallback propertyGraphElementTypeCallbackCallback,
 *     ArgumentBuilder args){
 *          if(this.isEdge()){
 *              return propertyGraphElementTypeCallbackCallback.edge(this, args);
 *          }else if(this.isVertex()){
 *              return propertyGraphElementTypeCallbackCallback.vertex(this, args);
 *          }else if(this.isNull()){
 *              return propertyGraphElementTypeCallbackCallback.nullElement(this, args);
 *          }
 *              return null;
 *      }
 * </code>
 * </pre>
 * <b>For a sample usage look at</b>
 * @see PropertyGraphElement
 * @code
 */
public interface PropertyGraphElementTypeCallback {
    public <T> T edge(PropertyGraphElement propertyGraphElement, ArgumentBuilder args);
    public <T> T vertex(PropertyGraphElement propertyGraphElement, ArgumentBuilder args);
    public <T> T nullElement(PropertyGraphElement propertyGraphElement, ArgumentBuilder args);
}
