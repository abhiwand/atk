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
package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema;

import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.io.Writable;

/**
 *  A common super class for {@code EdgeSchema} and {@code Propertyschema}, used for making type
 *  declarations for the graph database.
 */

/*  If we should ever need to make per-vertex declarations,
 *  we would make {@code VertexSchema} a subclass of this class. (And of course rename this class.)
 */

public abstract class EdgeOrPropertySchema implements Writable {

    /**
     * A tag used for distinguishing {@code EdgeSchema} in serialized representations.
     */
    public static final String EDGE_SCHEMA     = "EDGE_SCHEMA";

    /**
     * A tag used for distinguishing {@code PropertySchema} in serialized representations.
     */
    public static final String PROPERTY_SCHEMA = "PROPERTY_SCHEMA";

    /**
     * Returns the serialized identifier for this schema object.
     */
    public abstract StringType getID();
}
