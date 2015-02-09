package com.intel.giraph.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LdaMessage implements Writable {

    private LdaVertexId vid = new LdaVertexId();
    private VectorWritable vectorWritable = new VectorWritable();

    public LdaMessage() {
    }

    public LdaMessage(LdaVertexId vid, Vector vector) {
        this.vid = vid;
        this.vectorWritable.set(vector);
    }

    public LdaVertexId getVid() {
        return vid;
    }

    public Vector getVector() {
        return vectorWritable.get();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        vid.write(dataOutput);
        vectorWritable.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        vid.readFields(dataInput);
        vectorWritable.readFields(dataInput);
    }
}
