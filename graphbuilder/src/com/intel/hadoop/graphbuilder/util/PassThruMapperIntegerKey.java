package com.intel.hadoop.graphbuilder.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class PassThruMapperIntegerKey<VALUEIN, KEYOUT, VALUEOUT>
        extends Mapper<IntWritable, VALUEIN, KEYOUT, VALUEOUT> {
}
