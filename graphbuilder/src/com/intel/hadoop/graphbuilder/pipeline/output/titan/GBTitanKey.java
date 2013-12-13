/**
 * Copyright (C) 2013 Intel Corporation.
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

/**
 * An encapsulation of graphbuilder's view of a Titan key for indexing.
 * <p>Used in between parsing the command line and configuring Titan.</p>
 * @see TitanWriterMRChain
 */
public class GBTitanKey {

    private String   name;
    private Class<?> dataType;
    private boolean  isEdgeIndex;
    private boolean  isVertexIndex;
    private boolean  isUnique;

    /**
     * Construct a new Titan key from a string name and set other fields to default values.
     *
     * <p> Default values:
     * <ul>
     *     <li> {@code dataType}      : {@code String.class}</li>
     *     <li> {@code isEdgeIndex}   : {@code false}</li>
     *     <li> {@code isVertexIndex} : {@code false}</li>
     *     <li> {@code isUnique}      : {@code false}</li>
     * </ul></p>
     * @param name name of the property being associated with this Titan key
     */
    public GBTitanKey (String name) {
        this.name     = name;
        this.dataType = String.class;

        this.isEdgeIndex   = false;
        this.isVertexIndex = false;
        this.isUnique      = false;
    }

    /**
     * Construct a new Titan key with full specification of all fields - no default values will be provided.
     * @param dataType
     * @param isEdgeIndex
     * @param isVertexIndex
     * @param isUnique
     */
    public GBTitanKey(String name, Class<?> dataType, boolean isEdgeIndex, boolean isVertexIndex, boolean isUnique) {
        this.name          = name;
        this.dataType      = dataType;
        this.isEdgeIndex   = isEdgeIndex;
        this.isVertexIndex = isVertexIndex;
        this.isUnique      = isUnique;
    }

    /**
     * Get the name of the Titan key.
     * @return name of the Titan key.
     */
    public String getName() {
        return name;
    }

    /**
     * Set the name of the Titan key.
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the data type of the Titan key.
     * @return the key's datatype, a {@code Class<?>} object
     */
    public Class<?> getDataType() {
        return dataType;
    }

    /**
     * Set the data type of the Titan key.
     * @param dataType  the key's datatype, a {@code Class<?>} object
     */
    public void setDataType(Class<?>  dataType) {
        this.dataType = dataType;
    }

    /**
     * Is this key used for edge indexing?
     * @return  {@code true} iff the key is used for edge indexing
     */
    public boolean isEdgeIndex() {
        return isEdgeIndex;
    }

    /**
     * Set if this key is to be used for edge indexing.
     * @param isEdgeIndex {@code true} iff the key is to be used for edge indexing
     */
    public void setIsEdgeIndex(boolean isEdgeIndex) {
        this.isEdgeIndex = isEdgeIndex;
    }

    /**
     * Is this key to be used for vertex indexing?
     * @return {@code true} iff the key is to be used for vertex indexing
     */
    public boolean isVertexIndex() {
        return isVertexIndex;
    }

    /**
     * Set if this key is to be used for vertex indexing.
     * @param isVertexIndex  {@code true} iff the key is to be used for vertex indexing
     */
    public void setIsVertexIndex(boolean isVertexIndex) {
        this.isVertexIndex = isVertexIndex;
    }

    /**
     * Does this key take values that are unique per vertex?
     * <p>I.e. no two vertices can share a non-null value for this key</p>
     * @return  {@code true} if the key takes values that are unique per vertex
     */
    public boolean isUnique() {
        return isUnique;
    }

    /**
     * Set if this key is to take only values that are uniquely assigned to vertices.
     * @param isUnique  {@code true} if the key takes values that are unique per vertex
     */
    public void setIsUnique(boolean isUnique) {
        this.isUnique = isUnique;
    }
}
