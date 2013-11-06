package com.intel.giraph.io.titan.hbase;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;

import com.intel.giraph.io.titan.GiraphToTitanGraphFactory;

import org.apache.mahout.math.Vector;

import com.intel.giraph.io.EdgeDataWritable;
import com.intel.giraph.io.EdgeDataWritable.EdgeType;
import com.intel.giraph.io.VertexDataWritable;
import com.intel.giraph.io.VertexDataWritable.VertexType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.log4j.Logger;

import com.thinkaurelius.titan.diskstorage.Backend;

import java.io.IOException;
import java.nio.charset.Charset;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.*;
import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.EDGE_TYPE_PROPERTY_KEY;

/**
 * TitanHBaseVertexInputFormatPropertyGraph4CF loads vertex
 * Features <code>VertexData</code> vertex values and
 * <code>EdgeData</code> out-edge info.
 * <p/>
 * Each vertex follows this format:
 * (<vertex id>, <vertex valueVector>, <vertex property>,
 * ((<dest vertex id>, <edge value>, <edge property>), ...))
 * <p/>
 * Here is an example of left-side vertex, with vertex id 1,
 * vertex value 4,3 marked as "l", and two edges.
 * First edge has a destination vertex 2, edge value 2.1, marked as "tr".
 * Second edge has a destination vertex 3, edge value 0.7,marked as "va".
 * [1,[4,3],[l],[[2,2.1,[tr]],[3,0.7,[va]]]]
 */
public class TitanHBaseVertexInputFormatPropertyGraph4CF extends
        TitanHBaseVertexInputFormat<LongWritable, VertexDataWritable, EdgeDataWritable> {

    /**
     * the edge store name used by Titan
     */
    static final byte[] EDGE_STORE_FAMILY = Bytes.toBytes(Backend.EDGESTORE_NAME);
    /**
     * the vertex format type
     */
    static final String FORMAT_TYPE = "PropertyGraph4CF";
    /**
     * LOG class
     */
    private static final Logger LOG = Logger
            .getLogger(TitanHBaseVertexInputFormatPropertyGraph4CF.class);

    /**
     * checkInputSpecs
     *
     * @param conf : Giraph configuration
     */
    @Override
    public void checkInputSpecs(Configuration conf) {
    }

    /**
     * set up HBase with based on users' configuration
     *
     * @param conf :Giraph configuration
     */
    @Override
    public void setConf(
            ImmutableClassesGiraphConfiguration<LongWritable, VertexDataWritable, EdgeDataWritable> conf) {
        sanityCheckInputParameters(conf);
        conf.set(TableInputFormat.INPUT_TABLE, GIRAPH_TITAN_STORAGE_TABLENAME.get(conf));
        conf.set(HConstants.ZOOKEEPER_QUORUM, GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf));
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, GIRAPH_TITAN_STORAGE_PORT.get(conf));

        Scan scan = new Scan();
        scan.addFamily(Backend.EDGESTORE_NAME.getBytes(Charset.forName("UTF-8")));

        try {
            conf.set(TableInputFormat.SCAN, convertScanToString(scan));
        } catch (IOException e) {
            LOG.error("cannot write scan into a Base64 encoded string!");
        }

        super.setConf(conf);
    }

    /**
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    static String convertScanToString(Scan scan) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        scan.write(dos);
        return Base64.encodeBytes(out.toByteArray());
    }

    /**
     * check whether input parameter is valid
     *
     * @param conf : Giraph configuration
     */
    public void sanityCheckInputParameters(ImmutableClassesGiraphConfiguration<LongWritable, VertexDataWritable, EdgeDataWritable> conf) {
        String tableName = GIRAPH_TITAN_STORAGE_TABLENAME.get(conf);

        if (GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf).equals("")) {
            throw new IllegalArgumentException("Please configure Titan/HBase storage hostname by -D" +
                    GIRAPH_TITAN_STORAGE_HOSTNAME.getKey() + ". Otherwise no vertex will be read from Titan.");
        }

        if (tableName.equals("")) {
            throw new IllegalArgumentException("Please configure Titan/HBase Table name by -D" +
                    GIRAPH_TITAN_STORAGE_TABLENAME.getKey() + ". Otherwise no vertex will be read from Titan.");
        } else {
            try {
                HBaseConfiguration config = new HBaseConfiguration();
                HBaseAdmin hbaseAdmin = new HBaseAdmin(config);
                if (!hbaseAdmin.tableExists(tableName)) {
                    throw new IllegalArgumentException("HBase table " + tableName +
                            " does not exist! Please double check your configuration.");
                }

                if (!hbaseAdmin.isTableAvailable(tableName)) {
                    throw new IllegalArgumentException("HBase table " + tableName +
                            " is not available! Please double check your configuration.");
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to connect to HBase table " + tableName);
            }
        }


        if (GIRAPH_TITAN_STORAGE_PORT.isDefaultValue(conf)) {
            LOG.info(GIRAPH_TITAN_STORAGE_PORT.getKey() + " is configured as default value. " +
                    "Ensure you are using port " + GIRAPH_TITAN_STORAGE_PORT.get(conf));

        }

        if (INPUT_VERTEX_PROPERTY_KEY_LIST.get(conf).equals("")) {
            LOG.info("No input vertex property list specified. Ensure your " +
                    "InputFormat does not require one.");
        }

        if (INPUT_EDGE_PROPERTY_KEY_LIST.get(conf).equals("")) {
            LOG.info("No input edge property list specified. Ensure your " +
                    "InputFormat does not require one.");
        }

        if (INPUT_EDGE_LABEL_LIST.get(conf).equals("")) {
            LOG.info("No input edge label specified. Ensure your " +
                    "InputFormat does not require one.");
        }

        if (VERTEX_TYPE_PROPERTY_KEY.get(conf).equals("")) {
            LOG.info("No vertex type property specified. Ensure your " +
                    "InputFormat does not require one.");
        }

        if (EDGE_TYPE_PROPERTY_KEY.get(conf).equals("")) {
            LOG.info("No edge type property specified. Ensure your " +
                    "InputFormat does not require one.");
        }

    }

    /**
     * create TitanHBaseVertexReader
     *
     * @param split   : inputsplits from TableInputFormat
     * @param context : task context
     * @return VertexReader
     * @throws IOException
     * @throws RuntimeException
     */
    public VertexReader<LongWritable, VertexDataWritable, EdgeDataWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {

        return new TitanHBaseVertexReader(split, context);

    }

    /**
     * Uses the RecordReader to get HBase data
     */
    public static class TitanHBaseVertexReader extends
            HBaseVertexReader<LongWritable, VertexDataWritable, EdgeDataWritable> {
        /**
         * reader to parse Titan graph
         */
        private TitanHBaseGraphReader graphReader;
        /**
         * Giraph vertex
         */
        private Vertex<LongWritable, VertexDataWritable, EdgeDataWritable> vertex;
        /**
         * task context
         */
        private final TaskAttemptContext context;
        /**
         * The length of vertex value vector
         */
        private int cardinality = -1;
        /**
         * Data vector
         */
        private final Vector vector = null;

        /**
         * TitanHBaseVertexReader constructor
         *
         * @param split   Input Split
         * @param context Task context
         * @throws IOException
         */
        public TitanHBaseVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
            this.context = context;
        }

        /**
         * initialize TitanHBaseVertexReader
         *
         * @param inputSplit input splits
         * @param context    task context
         */
        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(inputSplit, context);
            this.graphReader = new TitanHBaseGraphReader(
                    GiraphToTitanGraphFactory.generateTitanConfiguration(context.getConfiguration(),
                            GIRAPH_TITAN.get(context.getConfiguration())));
        }

        /**
         * check whether there is next vertex
         *
         * @return boolean true if there is next vertex
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            if (getRecordReader().nextKeyValue()) {
                final Vertex<LongWritable, VertexDataWritable, EdgeDataWritable> temp = graphReader
                        .readGiraphVertex(FORMAT_TYPE, getConf(), getRecordReader()
                                .getCurrentKey().copyBytes(), getRecordReader().getCurrentValue().getMap()
                                .get(EDGE_STORE_FAMILY));
                if (null != temp) {
                    vertex = temp;
                    return true;
                } else if (getRecordReader().nextKeyValue()) {
                    final Vertex<LongWritable, VertexDataWritable, EdgeDataWritable> temp1 = graphReader
                            .readGiraphVertex(FORMAT_TYPE, getConf(), getRecordReader()
                                    .getCurrentKey().copyBytes(), getRecordReader().getCurrentValue().getMap()
                                    .get(EDGE_STORE_FAMILY));
                    if (null != temp1) {
                        vertex = temp1;
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * get current vertex with ID in long; value as two vectors, both in
         * double edge as two vectors, both in double
         *
         * @return Vertex Giraph vertex
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public Vertex<LongWritable, VertexDataWritable, EdgeDataWritable> getCurrentVertex()
                throws IOException, InterruptedException {
            return vertex;
        }

        /**
         * get vertex value
         *
         * @return TwoVectorWritable vertex value in two vectors
         * @throws IOException
         */
        protected VertexDataWritable getValue() throws IOException {
            VertexDataWritable vertexValue = vertex.getValue();
            Vector vector = vertexValue.getVector();
            if (cardinality != vector.size()) {
                if (cardinality == -1) {
                    cardinality = vector.size();
                } else {
                    throw new IllegalArgumentException("Error in input data:" + "different cardinality!");
                }
            }
            return vertexValue;
        }

        /**
         * get edges of this vertex
         *
         * @return Iterable of Giraph edges
         * @throws IOException
         */
        protected Iterable<Edge<LongWritable, EdgeDataWritable>> getEdges() throws IOException {
            return vertex.getEdges();
        }

    }
}
