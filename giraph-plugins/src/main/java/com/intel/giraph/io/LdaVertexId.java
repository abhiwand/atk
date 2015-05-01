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

public class LdaVertexId implements WritableComparable {

    private String value;

    /** vertex type has been embedded in the ID since it was needed for VID uniqueness */
    private boolean isDocument = false;

    public LdaVertexId() {
    }

    /**
     * LDA Vertex ID
     * @param value document name or word value
     * @param isDocument true if Document, false if vertex is for a Word
     */
    public LdaVertexId(String value, boolean isDocument) {
        this.value = value;
        this.isDocument = isDocument;
    }

    public String getValue() {
        return value;
    }

    public boolean isDocument() {
        return isDocument;
    }

    public boolean isWord() {
        return !isDocument();
    }

    /**
     * Not sure why but algorithm was using VertexId for getting a seed
     */
    public long seed() {
        return getValue().hashCode();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isDocument);
        dataOutput.writeUTF(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        isDocument = dataInput.readBoolean();
        value = dataInput.readUTF();
    }

    @Override
    public int compareTo(Object o) {
        LdaVertexId other = (LdaVertexId) o;
        if (isSameVertexType(other)) {
            return other.getValue().compareTo(getValue());
        }
        else if(isDocument()) {
            return 1;
        }
        else {
            return -1;
        }
    }

    private boolean isSameVertexType(LdaVertexId other) {
        return (isDocument() && other.isDocument()) &&
                (isWord() && other.isWord());
    }

    /**
     * Giraph re-uses writables, sometimes you need to make a copy()
     * @return copy of the current object with same values
     */
    public LdaVertexId copy() {
        return new LdaVertexId(value, isDocument);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !getClass().getName().equals(o.getClass().getName())) return false;

        LdaVertexId that = (LdaVertexId) o;

        if (isDocument != that.isDocument) return false;
        if (!value.equals(that.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = value.hashCode();
        result = 31 * result + (isDocument ? 1 : 0);
        return result;
    }


    @Override
    public String toString() {
        if (isDocument) {
            return "[D:" + value + "]";
        }
        else {
            return "[W:" + value + "]";
        }
    }
}
