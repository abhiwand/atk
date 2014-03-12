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

package com.intel.hadoop.graphbuilder.pipeline.output.titan.schemainference;

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.SchemaElement;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * A utility class that takes a multiset of <code>SchemaElement</code>objects, and merges so
 * into a duplicate-free list.
 * <p> Combination is done with the following semantics:
 * <li>
 * <ul>Two <code>PropertySchema</code> objects are identified if they have the same name.
 * They "combine" by throwing an exception  when two <code>Propertyschema</code> of the same name have different dataypes.</ul>
 * <ul>Two <code>EdgeSchema</code> objects are identified if they have the same label. They combine by merging their sets of
 * <code>PropertySchema</code></ul>
 * </li></p>
 */
public class MergeSchemataUtility {

    private static final Logger LOG = Logger.getLogger(MergeSchemataUtility.class);

    /**
     * Performs the merge of a multiset of <code>SchemaElement</code> objects into a duplicate free list.
     *
     * @param values The <code>SchemaElement</code> objects to be merged.
     * @return Duplicate-free list of <code>SchemaElement</code>'s.
     */

    public ArrayList<SchemaElement> merge(Iterable<SchemaElement> values) {

        HashMap<String, SchemaElement> schemaHashMap = new HashMap<String, SchemaElement>();

        for (SchemaElement schemaElement : values) {

            String schemaID = schemaElement.getID();

            if (schemaHashMap.containsKey(schemaID)) {
                schemaHashMap.get(schemaID).unionPropertySchemata(schemaElement.getPropertySchemata());
            } else {
                schemaHashMap.put(schemaID, new SchemaElement(schemaElement));
            }
        }

        ArrayList<SchemaElement> outValues = new ArrayList<SchemaElement>();
        outValues.addAll(schemaHashMap.values());

        return outValues;
    }
}
