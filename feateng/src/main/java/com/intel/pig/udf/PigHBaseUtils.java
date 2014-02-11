package com.intel.pig.udf;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;

import com.google.common.collect.Lists;

public class PigHBaseUtils {
	private static final Log LOG = LogFactory.getLog(PigHBaseUtils.class);

	private final static String HBASE_CONFIG_SET = "hbase.config.set";
	
	public static void initializeHBaseClassLoaderResources(Job job) throws IOException {
        // Depend on HBase to do the right thing when available, as of HBASE-9165
        try {
            Method addHBaseDependencyJars =
              TableMapReduceUtil.class.getMethod("addHBaseDependencyJars", Configuration.class);
            if (addHBaseDependencyJars != null) {
                addHBaseDependencyJars.invoke(null, job.getConfiguration());
                return;
            }
        } catch (NoSuchMethodException e) {
            LOG.debug("TableMapReduceUtils#addHBaseDependencyJars not available."
              + " Falling back to previous logic.", e);
        } catch (IllegalAccessException e) {
            LOG.debug("TableMapReduceUtils#addHBaseDependencyJars invocation"
              + " not permitted. Falling back to previous logic.", e);
        } catch (InvocationTargetException e) {
            LOG.debug("TableMapReduceUtils#addHBaseDependencyJars invocation"
              + " failed. Falling back to previous logic.", e);
        }
        // fall back to manual class handling.
        // Make sure the HBase, ZooKeeper, and Guava jars get shipped.
        TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
            org.apache.hadoop.hbase.client.HTable.class, // main hbase jar or hbase-client
            org.apache.hadoop.hbase.mapreduce.TableSplit.class, // main hbase jar or hbase-server
            com.google.common.collect.Lists.class, // guava
            org.apache.zookeeper.ZooKeeper.class); // zookeeper
        // Additional jars that are specific to v0.95.0+
        addClassToJobIfExists(job, "org.cloudera.htrace.Trace"); // htrace
        addClassToJobIfExists(job, "org.apache.hadoop.hbase.protobuf.generated.HBaseProtos"); // hbase-protocol
        addClassToJobIfExists(job, "org.apache.hadoop.hbase.TableName"); // hbase-common
        addClassToJobIfExists(job, "org.apache.hadoop.hbase.CompatibilityFactory"); // hbase-hadoop-compar
        addClassToJobIfExists(job, "org.jboss.netty.channel.ChannelFactory"); // netty
    }

    public static void addClassToJobIfExists(Job job, String className) throws IOException {
      Class klass = null;
      try {
          klass = Class.forName(className);
      } catch (ClassNotFoundException e) {
          LOG.debug("Skipping adding jar for class: " + className);
          return;
      }

      TableMapReduceUtil.addDependencyJars(job.getConfiguration(), klass);
    }
    
    @SuppressWarnings("unchecked")
    public static byte[] objToBytes(Object o, byte type, LoadCaster caster_) throws IOException {
        LoadStoreCaster caster = (LoadStoreCaster) caster_;
        if (o == null) return null;
        switch (type) {
        case DataType.BYTEARRAY: return ((DataByteArray) o).get();
        case DataType.BAG: return caster.toBytes((DataBag) o);
        case DataType.CHARARRAY: return caster.toBytes((String) o);
        case DataType.DOUBLE: return caster.toBytes((Double) o);
        case DataType.FLOAT: return caster.toBytes((Float) o);
        case DataType.INTEGER: return caster.toBytes((Integer) o);
        case DataType.LONG: return caster.toBytes((Long) o);
        case DataType.BIGINTEGER: return caster.toBytes((BigInteger) o);
        case DataType.BIGDECIMAL: return caster.toBytes((BigDecimal) o);
        case DataType.BOOLEAN: return caster.toBytes((Boolean) o);
        case DataType.DATETIME: return caster.toBytes((DateTime) o);

        // The type conversion here is unchecked.
        // Relying on DataType.findType to do the right thing.
        case DataType.MAP: return caster.toBytes((Map<String, Object>) o);

        case DataType.NULL: return null;
        case DataType.TUPLE: return caster.toBytes((Tuple) o);
        case DataType.ERROR: throw new IOException("Unable to determine type of " + o.getClass());
        default: throw new IOException("Unable to find a converter for tuple field " + o);
        }
    }
    
    public static JobConf initializeLocalJobConfig(Job job, Properties udfProps) {
        Configuration jobConf = job.getConfiguration();
        JobConf localConf = new JobConf(jobConf);
        if (udfProps.containsKey(HBASE_CONFIG_SET)) {
            for (Entry<Object, Object> entry : udfProps.entrySet()) {
                localConf.set((String) entry.getKey(), (String) entry.getValue());
            }
        } else {
            Configuration hbaseConf = HBaseConfiguration.create();
            for (Entry<String, String> entry : hbaseConf) {
                // JobConf may have some conf overriding ones in hbase-site.xml
                // So only copy hbase config not in job config to UDFContext
                // Also avoids copying core-default.xml and core-site.xml
                // props in hbaseConf to UDFContext which would be redundant.
                if (jobConf.get(entry.getKey()) == null) {
                    udfProps.setProperty(entry.getKey(), entry.getValue());
                    localConf.set(entry.getKey(), entry.getValue());
                }
            }
            udfProps.setProperty(HBASE_CONFIG_SET, "true");
        }
        return localConf;
    }

}
