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
package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema;

/**
 * Encapsulates the name and datatype of a property label.
 * <p>The expected use of this information is declaring keys for loading the constructed graph into a graph database.</p>
 */

public class PropertySchema {

    private String    name;

    private Class<?>  type;

    /**
     * Constructor.
     * @param name name of the property
     * @param type datatype of the property, a {@code Class<?>} object
     */
    public PropertySchema(String name, Class<?> type) {
        this.name = name;
        this.type = type;
    }

    /**
     * Set the name of the property.
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the name of the property.
     * @return
     */
    public String getName() {
        return this.name;
    }

    /**
     * Set the datatype of the property.
     * @param type  datatype of the property
     */
    public void setType(Class<?> type) {
        this.type = type;
    }

    /**
     * Get the datatype of the property
     * @return  datatype of the property
     */
    public Class<?> getType() {
        return this.type;
    }
}
