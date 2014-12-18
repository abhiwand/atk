package com.thinkaurelius.titan.hadoop.formats.titan_050.hbase;

import com.thinkaurelius.titan.hadoop.formats.titan_050.util.CachedTitanInputFormat;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * A temporary fix for Issue#817 KCVSLog$MessagePuller does not shut down when using the TitanInputFormat
 *
 * The Spark context does no shut down due to a runaway KCVSLog$MessagePuller thread that maintained a
 * connection to the underlying graph. Affects Titan 0.5.0 and 0.5.1. The bug was fixed in
 * Titan 0.5.2. However, we could not upgrade due to an issue in Titan 0.5.2 that caused bulk reads to hang on large datasets.
 *
 * This code is a copy of TitanHBaseInputFormat in Titan 0.5.0 that uses CachedTitanHBaseRecordReader.
 *
 * @link https://github.com/thinkaurelius/titan/issues/817
 */
public class CachedTitanHBaseInputFormat extends CachedTitanInputFormat {

    private final TableInputFormat tableInputFormat = new TableInputFormat();
    private byte[] edgestoreFamily;

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return this.tableInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new CachedTitanHBaseRecordReader(this.faunusConf, this.vertexQuery, (TableRecordReader) this.tableInputFormat.createRecordReader(inputSplit, taskAttemptContext), edgestoreFamily);
    }

    @Override
    public void setConf(final Configuration config) {
        super.setConf(config);


        //config.set(TableInputFormat.SCAN_COLUMN_FAMILY, Backend.EDGESTORE_NAME);
        config.set(TableInputFormat.INPUT_TABLE, titanInputConf.get(HBaseStoreManager.HBASE_TABLE));
        //config.set(HConstants.ZOOKEEPER_QUORUM, config.get(TITAN_HADOOP_GRAPH_INPUT_TITAN_STORAGE_HOSTNAME));
        config.set(HConstants.ZOOKEEPER_QUORUM, titanInputConf.get(GraphDatabaseConfiguration.STORAGE_HOSTS)[0]);
//        if (basicConf.get(TITAN_HADOOP_GRAPH_INPUT_TITAN_STORAGE_PORT, null) != null)
        if (titanInputConf.has(GraphDatabaseConfiguration.STORAGE_PORT))
            config.set(HConstants.ZOOKEEPER_CLIENT_PORT, String.valueOf(titanInputConf.get(GraphDatabaseConfiguration.STORAGE_PORT)));
        // TODO: config.set("storage.read-only", "true");
        config.set("autotype", "none");
        Scan scanner = new Scan();
        // TODO the mapping is private in HBaseStoreManager and leaks here -- replace String database/CF names with an enum where each value has both a short and long name
        if (titanInputConf.get(HBaseStoreManager.SHORT_CF_NAMES)) {
            scanner.addFamily("e".getBytes());
            edgestoreFamily = Bytes.toBytes("e");
        } else {
            scanner.addFamily(Backend.EDGESTORE_NAME.getBytes());
            edgestoreFamily = Bytes.toBytes(Backend.EDGESTORE_NAME);
        }

        //Bug fix: Moving creation of TitanHadoopSetup to Record reader
        //Need a way to setup filter without accessing graph. In Titan 0.5.0, setFilter does nothing
        //scanner.setFilter(getColumnFilter(titanSetup.inputSlice(this.vertexQuery)));

        //TODO (minor): should we set other options in http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Scan.html for optimization?
        Method converter;
        try {
            converter = TableMapReduceUtil.class.getDeclaredMethod("convertScanToString", Scan.class);
            converter.setAccessible(true);
            config.set(TableInputFormat.SCAN, (String) converter.invoke(null, scanner));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.tableInputFormat.setConf(config);
    }

    private Filter getColumnFilter(SliceQuery query) {
        return null;
        //TODO: return HBaseKeyColumnValueStore.getFilter(titanSetup.inputSlice(inputFilter));
    }

    @Override
    public Configuration getConf() {
        return tableInputFormat.getConf();
    }
}
