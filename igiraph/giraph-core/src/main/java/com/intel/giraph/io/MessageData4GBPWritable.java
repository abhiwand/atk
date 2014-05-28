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
 * Writable to handle serialization of the fields associated with MessageData4GBP
 */
public class MessageData4GBPWritable implements Writable {

    /** The vertex id for this message */
    private long id = 0L;
    /** The Gaussian distribution of this message */
    private GaussianDistWritable gauss = new GaussianDistWritable();

    /**
     * Default constructor
     */
    public MessageData4GBPWritable() {
    }

    /**
     * Constructor
     *
     * @param id of type long
     * @param mean of type double
     * @param precision of type double
     */
    public MessageData4GBPWritable(long id, double mean, double precision) {
        this.id = id;
        gauss.setMean(mean);
        gauss.setPrecision(precision);
    }

    /**
     * Constructor
     *
     * @param id of type long
     * @param gauss of type GaussianDistWritable
     */
    public MessageData4GBPWritable(long id, GaussianDistWritable gauss) {
        this.id = id;
        this.gauss.set(gauss);
    }

    /**
     * Setter
     *
     * @param id of type long
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * Getter
     *
     * @return id of type long
     */
    public long getId() {
        return id;
    }

    /**
     * Setter
     *
     * @param gauss of type GaussianDistWritable
     */
    public void setGauss(GaussianDistWritable gauss) {
        this.gauss.set(gauss);
    }

    /**
     * Getter
     *
     * @return gauss of type GaussianDistWritable
     */
    public GaussianDistWritable getGauss() {
        return gauss;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        gauss.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        gauss.write(out);
    }

    /**
     * Read message data from DataInput
     *
     * @param in of type DataInput
     * @return MessageData4GBPWritable
     * @throws IOException
     */
    public static MessageData4GBPWritable read(DataInput in) throws IOException {
        MessageData4GBPWritable writable = new MessageData4GBPWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write message data to DataOutput
     *
     * @param out of type DataOutput
     * @param id of type long
     * @param mean of type double
     * @param precision of type double
     * @throws IOException
     */
    public static void write(DataOutput out, long id, double mean, double precision) throws IOException {
        new MessageData4GBPWritable(id, mean, precision).write(out);
    }

}
