/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.intel.pig.udf;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.backend.hadoop.hbase.HBaseBinaryConverter;

/**
 * An HBase StoreFunc for deleting rows from a specified HBase table
 * <P>
 * <pre>{@code
 * raw = LOAD 'hbase://SampleTable'
 *       USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
 *       'info:first_name info:last_name friends:* info:*', '-loadKey true -limit 5')
 *       AS (id:bytearray, first_name:chararray, last_name:chararray, friends_map:map[], info_map:map[]);
 * }</pre>
 * This example loads data redundantly from the info column family just to
 * illustrate usage. Note that the row key is inserted first in the result schema.
 * To load only column names that start with a given prefix, specify the column
 * name with a trailing '*'. For example passing <code>friends:bob_*</code> to
 * the constructor in the above example would cause only columns that start with
 * <i>bob_</i> to be loaded.
 * <P>
 * Note that when using a prefix like <code>friends:bob_*</code>, explicit HBase filters are set for
 * all columns and prefixes specified. Querying HBase with many filters can cause performance
 * degredation. This is typically seen when mixing one or more prefixed descriptors with a large list
 * of columns. In that case better perfomance will be seen by either loading the entire family via
 * <code>friends:*</code> or by specifying explicit column descriptor names.
 * <P>
 * Below is an example showing how to store data into HBase:
 * <pre>{@code
 * copy = STORE raw INTO 'hbase://SampleTableCopy'
 *       USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
 *       'info:first_name info:last_name friends:* info:*');
 * }</pre>
 * Note that STORE will expect the first value in the tuple to be the row key.
 * Scalars values need to map to an explicit column descriptor and maps need to
 * map to a column family name. In the above examples, the <code>friends</code>
 * column family data from <code>SampleTable</code> will be written to a
 * <code>buddies</code> column family in the <code>SampleTableCopy</code> table.
 *
 */
public class HBaseDelete extends StoreFunc {

    private static final Log LOG = LogFactory.getLog(HBaseDelete.class);

    //Use JobConf to store hbase delegation token
    private JobConf m_conf;
    private RecordWriter writer;
    private TableOutputFormat outputFormat = null;
    private String contextSignature = null;
    private LoadStoreCaster caster = new HBaseBinaryConverter();
    private ResourceSchema schema_;
    
    /**
     * Returns UDFProperties based on <code>contextSignature</code>.
     */
    private Properties getUDFProperties() {
        return UDFContext.getUDFContext()
            .getUDFProperties(this.getClass(), new String[] {contextSignature});
    }
    
    /*
     * StoreFunc Methods
     * @see org.apache.pig.StoreFuncInterface#getOutputFormat()
     */

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        if (outputFormat == null) {
            if (m_conf == null) {
                throw new IllegalStateException("setStoreLocation has not been called");
            } else {
                this.outputFormat = new TableOutputFormat();
                this.outputFormat.setConf(m_conf);
            }
        }
        return outputFormat;
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        schema_ = s;
        getUDFProperties().setProperty(contextSignature + "_schema",
                                       ObjectSerializer.serialize(schema_));
    }

    // Suppressing unchecked warnings for RecordWriter, which is not parameterized by StoreFuncInterface
    @Override
    public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    // Suppressing unchecked warnings for RecordWriter, which is not parameterized by StoreFuncInterface
    @SuppressWarnings("unchecked")
    @Override
    public void putNext(Tuple t) throws IOException {
        ResourceFieldSchema[] fieldSchemas = (schema_ == null) ? null : schema_.getFields();
        byte type = (fieldSchemas == null) ? DataType.findType(t.get(0)) : fieldSchemas[0].getType();
    	byte[] rowKey = PigHBaseUtils.objToBytes(t.get(0), type, caster);
                        
        try {
        	System.out.println("deleting row with key " + Bytes.toString(rowKey));
        	Delete delete = new Delete(rowKey);
            writer.write(null, delete);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir)
    throws IOException {
        return location;
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        this.contextSignature = signature;
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        if (location.startsWith("hbase://")){
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, location.substring(8));
        }else{
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, location);
        }

        String serializedSchema = getUDFProperties().getProperty(contextSignature + "_schema");
        if (serializedSchema!= null) {
            schema_ = (ResourceSchema) ObjectSerializer.deserialize(serializedSchema);
        }

        PigHBaseUtils.initializeHBaseClassLoaderResources(job);
        m_conf = PigHBaseUtils.initializeLocalJobConfig(job, getUDFProperties());
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
    }

    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
    }
}
