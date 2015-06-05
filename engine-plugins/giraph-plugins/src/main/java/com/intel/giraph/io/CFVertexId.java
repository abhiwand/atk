//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.giraph.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CFVertexId implements WritableComparable {

    private String value;

    /** vertex type has been embedded in the ID since it was needed for VID uniqueness */
    private boolean isUserVertex = false;

    public CFVertexId() {
    }

    /**
     *
     * @param value the value of vertex
     * @param isUser true if this is a user vertex; false otherwise
     */
    public CFVertexId(String value, boolean isUser) {
        this.value = value;
        this.isUserVertex = isUser;
    }

    public String getValue() {
        return value;
    }

    public Long getValueAsLong() {
        return Long.getLong(value, 0);
    }

    public boolean isUser() {
        return isUserVertex;
    }

    public boolean isItem() {
        return false == isUser();
    }

    /**
     *
     * @return the object has code
     */
    public long seed() {
        return getValue().hashCode();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isUserVertex);
        dataOutput.writeUTF(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        isUserVertex = dataInput.readBoolean();
        value = dataInput.readUTF();
    }

    @Override
    public int compareTo(Object o) {
        CFVertexId other = (CFVertexId) o;
        if (isSameVertexType(other)) {
            return other.getValue().compareTo(getValue());
        }
        else if(isUser()) {
            return 1;
        }
        else {
            return -1;
        }
    }

    private boolean isSameVertexType(CFVertexId other) {
        return (isUser() && other.isUser()) &&
                (isItem() && other.isItem());
    }

    /**
     * Giraph re-uses writables, sometimes you need to make a copy()
     * @return copy of the current object with same values
     */
    public CFVertexId copy() {
        return new CFVertexId(value, isUserVertex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !getClass().getName().equals(o.getClass().getName())) return false;

        CFVertexId that = (CFVertexId) o;

        if (isUserVertex != that.isUserVertex) return false;
        if (!value.equals(that.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = value.hashCode();
        result = 31 * result + (isUserVertex ? 1 : 0);
        return result;
    }


    @Override
    public String toString() {
        if (isUserVertex) {
            return "[User:" + value + "]";
        }
        else {
            return "[Item:" + value + "]";
        }
    }
}
