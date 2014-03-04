/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder
 */
package com.intel.pig.udf.util;

import java.io.IOException;
import java.util.Hashtable;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class InputTupleInProgress {
    private Tuple tuple;
    private Schema inputSchema;
    private Hashtable<String, Byte> fieldNameToDataType;

    public InputTupleInProgress(Tuple input, Schema inputSchema) {
        this.tuple = input;
        this.inputSchema = inputSchema;
        this.fieldNameToDataType = mapFieldNamesAndTypes();
    }

    private Hashtable<String, Byte> mapFieldNamesAndTypes() {
        Hashtable<String, Byte> nameToTypeMap = new Hashtable<String, Byte>();
        for (Schema.FieldSchema field : this.inputSchema.getFields()) {
            nameToTypeMap.put(field.alias, field.type);
        }
        return nameToTypeMap;
    }

    public Byte getType(String fieldName) {
        return this.fieldNameToDataType.get(fieldName);
    }

    /**
     * Get an element from a tuple
     * @param fieldName name of field to get from tuple
     * @return an element from the tuple
     */
    public Object getTupleData(String fieldName) throws IOException {

        int fieldPos = this.inputSchema.getPosition(fieldName);
        if (fieldPos < 0) {
            throw new IllegalArgumentException("Did NOT find field named: " + fieldName + " in input schema");
        }
        return this.tuple.get(fieldPos);
    }

}
