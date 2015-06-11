/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.giraph.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of the fields associated with EdgeData4CF
 */
public class EdgeData4GBPWritable implements Writable {

    /** The weight value at this edge */
    private double weight = 0d;

    /** The weight value on the dst->src edge */
    private double reverseWeight = 0d;

    /**
     * Default constructor
     */
    public EdgeData4GBPWritable() {
    }

    /**
     * Constructor
     *
     * @param weight of type double
     * @param reverseWeight of type double
     */
    public EdgeData4GBPWritable(double weight, double reverseWeight) {
        this.weight = weight;
        this.reverseWeight = reverseWeight;
    }

    /**
     * Setter
     *
     * @param reverseWeight of type double
     */
    public void setReverseWeight(double reverseWeight) {
        this.reverseWeight = reverseWeight;
    }

    /**
     * Getter
     *
     * @return reverseWeight
     */
    public double getReverseWeight() {
        return reverseWeight;
    }

    /**
     * Setter
     *
     * @param weight of type double
     */
    public void setWeight(double weight) {
        this.weight = weight;
    }

    /**
     * Getter
     *
     * @return weight of type double
     */
    public double getWeight() {
        return weight;
    }

    /**
     * Get reverse edge data
     * @return reverseEdgeData of type EdgeData4GBPWritalbe
     */
    public EdgeData4GBPWritable getReverseEdgeData() {
        EdgeData4GBPWritable reverseEdgeData = new EdgeData4GBPWritable();
        reverseEdgeData.setWeight(reverseWeight);
        reverseEdgeData.setReverseWeight(weight);
        return reverseEdgeData;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setWeight(in.readDouble());
        setReverseWeight(in.readDouble());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(getWeight());
        out.writeDouble(getReverseWeight());
    }

    /**
     * Read edge data from DataInput
     *
     * @param in of type DataInput
     * @return EdgeDataWritable
     * @throws IOException
     */
    public static EdgeData4GBPWritable read(DataInput in) throws IOException {
        EdgeData4GBPWritable writable = new EdgeData4GBPWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write edge data to DataOutput
     *
     * @param out of type DataOutput
     * @param weight of type double
     * @param reverseWeight of type double
     * @throws IOException
     */
    public static void write(DataOutput out, double weight, double reverseWeight) throws IOException {
        new EdgeData4GBPWritable(weight, reverseWeight).write(out);
    }

}
