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
 * Writable to handle serialization of the fields associated with GBP vertex data
 */
public class VertexData4GBPWritable implements Writable {
    /** Gaussian prior */
    private GaussianDistWritable prior = new GaussianDistWritable();
    /** Gaussian posterior */
    private GaussianDistWritable posterior = new GaussianDistWritable();
    /** Gaussian intermediate to iterative computation*/
    private GaussianDistWritable intermediate = new GaussianDistWritable();
    /** double mean from previous step */
    private double prevMean = 0d;

    /**
     * Default constructor
     */
    public VertexData4GBPWritable() {
    }

    /**
     * Constructor
     *
     * @param prior of type GaussianDistWritable
     * @param posterior of type GaussianDistWritable
     * @param intermediate of type GaussianDistWritable
     * @param prevMean of type double
     */
    public VertexData4GBPWritable(GaussianDistWritable prior, GaussianDistWritable posterior,
                                  GaussianDistWritable intermediate, double prevMean) {
        setPrior(prior);
        setPosterior(posterior);
        setIntermediate(intermediate);
        setPrevMean(prevMean);
    }

    /**
     * Getter
     *
     * @return prior of type GaussianDistWritable
     */
    public GaussianDistWritable getPrior() {
        return prior;
    }

    /**
     * Setter
     *
     * @param prior of type GaussianDistWritable
     */
    public void setPrior(GaussianDistWritable prior) {
        this.prior.set(prior);
    }

    /**
     * Getter
     *
     * @return posterior of type GaussianDistWritable
     */
    public GaussianDistWritable getPosterior() {
        return posterior;
    }

    /**
     * Setter
     *
     * @param posterior of type GaussianDistWritable
     */
    public void setPosterior(GaussianDistWritable posterior) {
        this.posterior.set(posterior);
    }

    /**
     * Getter
     *
     * @return intermediate of type GaussianDistWritable
     */
    public GaussianDistWritable getIntermediate() {
        return intermediate;
    }

    /**
     * Setter
     *
     * @param intermediate of type GaussianDistWritable
     */
    public void setIntermediate(GaussianDistWritable intermediate) {
        this.intermediate.set(intermediate);
    }

    /**
     * Getter
     *
     * @return prevMean of type double
     */
    public double getPrevMean() {
        return prevMean;
    }

    /**
     * Setter
     *
     * @param prevMean of type double
     */
    public void setPrevMean(double prevMean) {
        this.prevMean = prevMean;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        prior.readFields(in);
        posterior.readFields(in);
        intermediate.readFields(in);
        setPrevMean(in.readDouble());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        prior.write(out);
        posterior.write(out);
        intermediate.write(out);
        out.writeDouble(getPrevMean());
    }

    /**
     * Read vertex data from DataInput
     *
     * @param in of type DataInput
     * @return VertexData4GBPWritable
     * @throws IOException
     */
    public static VertexData4GBPWritable read(DataInput in) throws IOException {
        VertexData4GBPWritable writable = new VertexData4GBPWritable();
        writable.readFields(in);
        return writable;
    }

    /**
     * Write vertex data to DataOutput
     *
     * @param out of type DataOutput
     * @param prior of type GaussianDistWritable
     * @param posterior of type GaussianDistWritable
     * @param intermediate of type GaussianDistWritable
     * @param prevMean of type double
     * @throws IOException
     */
    public static void write(DataOutput out, GaussianDistWritable prior,
        GaussianDistWritable posterior, GaussianDistWritable intermediate,
        double prevMean) throws IOException {
        new VertexData4GBPWritable(prior, posterior, intermediate, prevMean).write(out);
    }

}
