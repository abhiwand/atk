/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.intel.daal.algorithms;

import com.intel.daal.algorithms.linear_regression.Model;
import com.intel.daal.services.DaalContext;
import java.nio.ByteBuffer;

/**
 * Serializer/Deserializer for DAAL models
 */
public final class ModelSerializer {
    static {
        System.loadLibrary("AtkDaalJavaAPI");
    }

    /**
     * Serialize DAAL linear regression QR model
     *
     * @param model Linear regression model
     * @return Serialized model
     */
    public static byte[] serializeQrModel(com.intel.daal.algorithms.linear_regression.Model model) {
        ByteBuffer buffer = cSerializeQrModel(model.getCModel());
        byte[] serializedCObject = new byte[buffer.capacity()];
        buffer.position(0);
        buffer.get(serializedCObject);
        cFreeByteBuffer(buffer);
        return serializedCObject;
    }

    /**
     * Deserialize DAAL linear regression QR model
     *
     * @param context DAAL context
     * @param serializedCObject Serialized model
     * @return Deserialized model
     */
    public static com.intel.daal.algorithms.linear_regression.Model deserializeQrModel(DaalContext context, byte[] serializedCObject) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(serializedCObject.length);
        buffer.put(serializedCObject);

        long cModelRef = cDeserializeQrModel(buffer, buffer.capacity());
        Model qrModel = new Model(context, cModelRef);
        return qrModel;
    }

    protected static native ByteBuffer cSerializeQrModel(long cModel);

    protected static native long cDeserializeQrModel(ByteBuffer buffer, long size);

    private static native void cFreeByteBuffer(ByteBuffer var1);
}
