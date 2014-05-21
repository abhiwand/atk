//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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
