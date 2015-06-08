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
