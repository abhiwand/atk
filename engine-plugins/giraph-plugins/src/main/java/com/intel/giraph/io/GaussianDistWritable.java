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
 * Writable to handle serialization of the fields associated with Gaussian Dist.
 */
public class GaussianDistWritable implements Writable {

    /** mean of Gaussian distribution */
    private double mean = 0d;
    /** precision of Gaussian distribution */
    private double precision = 0d;

    /**
     * Default constructor
     */
    public GaussianDistWritable() {
    }

    /**
     * Constructor
     *
     * @param mean of type double
     * @param precision of type double
     */
    public GaussianDistWritable(double mean, double precision) {
        this.mean = mean;
        this.precision = precision;
    }

    /**
     * Getter
     *
     * @return mean of type double
     */
    public double getMean() {
        return mean;
    }

    /**
     * Setter
     *
     * @param mean of type double
     */
    public void setMean(double mean) {
        this.mean = mean;
    }

    /**
     * Getter
     *
     * @return precision of type double
     */
    public double getPrecision() {
        return precision;
    }

    /**
     * Setter
     *
     * @param precision of type double
     */
    public void setPrecision(double precision) {
        this.precision = precision;
    }

    /**
     * Setter
     *
     * @param gauss of type GaussianDistWritable
     */
    public void set(GaussianDistWritable gauss) {
        this.mean = gauss.mean;
        this.precision = gauss.precision;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        mean = in.readDouble();
        precision = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(mean);
        out.writeDouble(precision);
    }

    /**
     * Read vertex data from DataInput
     *
     * @param in of type DataInput
     * @return GaussianDistWritable
     * @throws IOException
     */
    public static GaussianDistWritable read(DataInput in) throws IOException {
        GaussianDistWritable writable = new GaussianDistWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write vertex data to DataOutput
     *
     * @param out of type DataOutput
     * @param mean of type double
     * @param precision of type double
     * @throws IOException
     */
    public static void write(DataOutput out, double mean, double precision) throws IOException {
        new GaussianDistWritable(mean, precision).write(out);
    }

}
