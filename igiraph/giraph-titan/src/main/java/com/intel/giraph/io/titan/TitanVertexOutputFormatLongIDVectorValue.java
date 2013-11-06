package com.intel.giraph.io.titan;

import com.intel.mahout.math.TwoVectorWritable;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;

import static com.intel.giraph.io.titan.conf.GiraphTitanConstants.*;

/**
 * The Vertex Output Format which writes back Giraph algorithm results
 * to Tian.
 * <p/>
 * Each Vertex is with <code>Long</code> id,
 * and <code>TwoVector</code> values.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */

public class TitanVertexOutputFormatLongIDVectorValue<I extends LongWritable,
        V extends TwoVectorWritable, E extends Writable>
        extends TextVertexOutputFormat<I, V, E> {

    /**
     * LOG class
     */
    private static final Logger LOG = Logger
            .getLogger(TitanVertexOutputFormatLongIDVectorValue.class);


    /**
     * set up Titan with based on users' configuration
     *
     * @param conf : Giraph configuration
     */
    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        sanityCheckInputParameters(conf);
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        super.setConf(conf);
    }

    /**
     * check whether input parameter is valid
     *
     * @param conf : Giraph configuration
     */
    public void sanityCheckInputParameters(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        String[] vertexPropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(conf).split(",");
        if (vertexPropertyKeyList.length == 0) {
            throw new IllegalArgumentException("Please configure output vertex property list by -D" +
                    OUTPUT_VERTEX_PROPERTY_KEY_LIST.getKey() + ". Otherwise no vertex result will be written.");
        }

        if (GIRAPH_TITAN_STORAGE_BACKEND.get(conf).equals("")) {
            throw new IllegalArgumentException("Please configure Titan storage backend by -D" +
                    GIRAPH_TITAN_STORAGE_BACKEND.getKey() + ". Otherwise no vertex will be read from Titan.");
        }

        if (GIRAPH_TITAN_STORAGE_TABLENAME.get(conf).equals("")) {
            throw new IllegalArgumentException("Please configure Titan storage Table name by -D" +
                    GIRAPH_TITAN_STORAGE_TABLENAME.getKey() + ". Otherwise no vertex will be read from Titan.");
        }

        if (GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf).equals("")) {
            throw new IllegalArgumentException("Please configure Titan storage hostname by -D" +
                    GIRAPH_TITAN_STORAGE_HOSTNAME.getKey() + ". Otherwise no vertex will be read from Titan.");
        }

        if (GIRAPH_TITAN_STORAGE_PORT.isDefaultValue(conf)){
            LOG.info(GIRAPH_TITAN_STORAGE_PORT.getKey() + " is configured as default value. " +
                    "Ensure you are using port " + GIRAPH_TITAN_STORAGE_PORT.get(conf));
        }

        if (GIRAPH_TITAN_STORAGE_READ_ONLY.get(conf).equals("true")) {
            throw new IllegalArgumentException("Please turnoff Titan storage read-only by -D" +
                    GIRAPH_TITAN_STORAGE_READ_ONLY.getKey() + ". Otherwise no vertex will be read from Titan.");
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

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new TitanHBaseLongIDTwoVectorValueWriter();
    }

    /**
     * VertexWriter that writes Giraph results to Titan via BluePrint API
     * vertices with <code>Long</code> id
     * and <code>TwoVector</code> values.
     */
    protected class TitanHBaseLongIDTwoVectorValueWriter extends TextVertexWriterToEachLine {

        /**
         * reader to parse Titan graph
         */
        private TitanGraph graph;
        /**
         * Vertex properties to filter
         */
        private String[] vertexPropertyKeyList;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(context);
            this.graph = TitanGraphWriter.open(context);
            assert (null != this.graph);
            vertexPropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(context.getConfiguration()).split(",");
            for (int i = 0; i < vertexPropertyKeyList.length; i++) {
                LOG.info("create vertex.property in Titan " + vertexPropertyKeyList[i]);
                //     this.graph.makeType().name().unique(Direction.OUT).dataType(String.class)
                //             .makePropertyKey();
                this.graph.makeKey(vertexPropertyKeyList[i]).dataType(String.class).make();

                //  this.graph.makeType().name(vertexPropertyKeyList[i]).unique(Direction.OUT).dataType(Double.class)
                //        .makePropertyKey();
            }
        }


        @Override
        public Text convertVertexToLine(Vertex<I, V, E> vertex) throws IOException {

            long vertex_id = vertex.getId().get();
            com.tinkerpop.blueprints.Vertex bluePrintVertex = this.graph.getVertex(vertex_id);
            Vector vector = vertex.getValue().getPosteriorVector();
            JSONArray jsonVertex = new JSONArray();


            if (vector.size() == vertexPropertyKeyList.length) {
                try {
                    jsonVertex.put(vertex_id);
                    JSONArray jsonValueArray = new JSONArray();
                    for (int i = 0; i < vector.size(); i++) {
                        jsonValueArray.put(vector.getQuick(i));
                        bluePrintVertex.setProperty(vertexPropertyKeyList[i], Double.toString(vector.getQuick(i)));
                        //bluePrintVertex.setProperty(vertexPropertyKeyList[i], vector.getQuick(i));
                    }
                    this.graph.commit();
                    jsonVertex.put(jsonValueArray);
                } catch (JSONException e) {
                    throw new IllegalArgumentException("writeVertex: Couldn't write vertex " + vertex);
                }
            } else {
                LOG.error("The number of output vertex property does not match! " +
                        "The size of vertex value vector is : " + vector.size() +
                        ", The size of output vertex property is: " + vertexPropertyKeyList.length);
                throw new IllegalArgumentException("The number of output vertex property does not match. Current Vertex is: " + vertex.getId());
            }

            return new Text(jsonVertex.toString());
        }
    }
}
