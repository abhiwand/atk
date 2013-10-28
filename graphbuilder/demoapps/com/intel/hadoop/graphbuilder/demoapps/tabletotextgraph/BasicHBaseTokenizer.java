

package com.intel.hadoop.graphbuilder.demoapps.tabletotextgraph;

import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.RecordTypeHBaseRow;
import com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.GBHTableConfig;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import javax.xml.parsers.ParserConfigurationException;
import java.util.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BasicHBaseTokenizer implements GraphTokenizer<RecordTypeHBaseRow, StringType> {

    private static final Logger LOG = Logger.getLogger(BasicHBaseTokenizer.class);

    private List<String>                  vertexIdColumnList;
    private HashMap<String, String[]>     vertexPropColMap;
    private ArrayList<Vertex<StringType>> vertexList;

    private HashMap<String, EdgeRule>     edgeLabelToEdgeRules;
    private ArrayList<String>             edgeLabelList;
    private ArrayList<Edge<StringType>>   edgeList;

    private class EdgeRule {
        private String       srcColumnName;
        private String       dstColumnName;
        private List<String> propertyColumnNames;
        boolean              isBiDirectional;

        EdgeRule() {
            srcColumnName       = null;
            dstColumnName       = null;
            propertyColumnNames = new ArrayList<String>();
            isBiDirectional     = false;
        }

        void setSrcColumnName(String name) {
            this.srcColumnName = name;
        }

        String getSrcColumnName() {
            return this.srcColumnName;
        }

        void setDstColumnName(String name) {
            this.dstColumnName = name;
        }

        String getDstColumnName() {
            return this.dstColumnName;
        }

        void addPropertyColumnName(String columnName) {
            propertyColumnNames.add(columnName);
        }

        List<String> getPropertyColumnNames() {
            return propertyColumnNames;
        }

        void setBiDirectional(boolean isBiDirectional) {
            this.isBiDirectional = isBiDirectional;
        }

        boolean isBiDirectional() {
            return this.isBiDirectional;
        }
    }

    public BasicHBaseTokenizer() throws ParserConfigurationException {

        vertexPropColMap   = new HashMap<String, String[]>();
        vertexIdColumnList = new ArrayList<String>();
        vertexList         = new ArrayList<Vertex<StringType>>();

        edgeLabelToEdgeRules  = new HashMap<String, EdgeRule>();
        edgeLabelList         = new ArrayList<String>();
        edgeList              = new ArrayList<Edge<StringType>>();
    }

    @Override
    public void configure(Configuration conf) {
        // Parse the column names of vertices and properties from command line prompt
        // <vertex_col1>=[<vertex_prop1>,...] [<vertex_col2>=[<vertex_prop1>,...]]

        String   separators          = "\\" + GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR");
        String[] vertexIdColumnNames = conf.get(GBHTableConfig.config.getProperty("VCN_CONF_NAME")).split(separators);
        String   vertexIdColumnName  = null;

        for (String iteratorString : vertexIdColumnNames) {
            if (iteratorString.contains("=")) {
                String[] columnNames = iteratorString.split("\\=");

                vertexIdColumnName = columnNames[0];
                vertexIdColumnList.add(vertexIdColumnName);

                String[] vertexPropertiesColumnNames = columnNames[1].split("\\,");
                vertexPropColMap.put(vertexIdColumnName, vertexPropertiesColumnNames);
            } else {
                vertexIdColumnName = iteratorString;
                vertexIdColumnList.add(vertexIdColumnName);
            }
        }

        LOG.info("TRIBECA_INFO: Number of vertices to be read from HBase = " + vertexIdColumnList.size());

        // now we have to do the same with the edges
        // The first three attributes in the edge string are source vertex column, destination
        // vertex column and the string label e.g.
        // <src_vertex_col>,<dest_vertex_col>,<label>,[<edge_property_col1>,<edge_property_col2>,...]

        String   separator   = "\\" + GBHTableConfig.config.getProperty("COL_NAME_SEPARATOR");

        String edgeSpecification =  conf.get(GBHTableConfig.config.getProperty("ECN_CONF_NAME"));

        if (edgeSpecification != null) {
            String[] edgeStrings = edgeSpecification.split(separator);

            for (String next : edgeStrings) {

                String[] columnNames      = next.split("\\,");
                String   srcVertexColName = columnNames[0];
                String   tgtVertexColName = columnNames[1];
                String   label            = columnNames[2];
                EdgeRule edgeRule         = new EdgeRule();

                edgeRule.setBiDirectional(true);
                edgeRule.setSrcColumnName(srcVertexColName);
                edgeRule.setDstColumnName(tgtVertexColName);

                for (int i = 3; i < columnNames.length; i++) {
                    edgeRule.addPropertyColumnName(columnNames[i]);
                }

                edgeLabelToEdgeRules.put(label, edgeRule);
                edgeLabelList.add(label);
            }
        }

        String directedEdgeSpecification = conf.get(GBHTableConfig.config.getProperty("DECN_CONF_NAME"));

        if (directedEdgeSpecification != null) {
            String[] directedEdgeStrings = directedEdgeSpecification.split(separator);

            for (String next : directedEdgeStrings) {

                String[] columnNames      = next.split("\\,");
                String   srcVertexColName = columnNames[0];
                String   tgtVertexColName = columnNames[1];
                String   label            = columnNames[2];
                EdgeRule edgeRule         = new EdgeRule();

                edgeRule.setBiDirectional(false);
                edgeRule.setSrcColumnName(srcVertexColName);
                edgeRule.setDstColumnName(tgtVertexColName);

                for (int i = 3; i < columnNames.length; i++) {
                    edgeRule.addPropertyColumnName(columnNames[i]);
                }

                edgeLabelToEdgeRules.put(label, edgeRule);
                edgeLabelList.add(label);
            }
        }
    }

    @Override
    public Class vidClass() {
        return StringType.class;
    }

    /**
     * @param columns        HTable columns for the current row
     * @param fullColumnName Name of the HTABLE column - <column family>:<column qualifier>
     * @param context        Hadoop's mapper context
     */
    private String getColumnData(Result columns, String fullColumnName, Mapper.Context context) {

        String value = Bytes.toString(HBaseUtils.getColumnData(columns, fullColumnName));

        if (null != value) {
            context.getCounter(GBHTableConfig.Counters.HTABLE_COLS_READ).increment(1);

            if (value.isEmpty()) {
                context.getCounter(GBHTableConfig.Counters.HTABLE_COLS_IGNORED).increment(1l);
                value = null;
            }
        } else {
            context.getCounter(GBHTableConfig.Counters.HTABLE_COL_READ_ERROR).increment(1l);
        }

        return value;
    }

    public void parse(RecordTypeHBaseRow record, Mapper.Context context) {

        ImmutableBytesWritable row     = record.getRow();
        Result                 columns = record.getColumns();

        vertexList.clear();
        edgeList.clear();

        try {

            // check row for vertices

            for (String columnName : vertexIdColumnList) {

                String vertexId = getColumnData(columns, columnName, context);

                if (null != vertexId) {

                    // create vertex

                    Vertex<StringType> vertex = new Vertex<StringType>(new StringType(vertexId));

                    // add the vertex properties

                    String[] vpColNames = vertexPropColMap.get(columnName);

                    if (null != vpColNames) {

                        String value = null;

                        if (vpColNames.length > 0) {
                            for (String vertexPropertyColumnName : vpColNames) {
                                value =  getColumnData(columns, vertexPropertyColumnName, context);
                                if (value != null) {
                                    vertex.setProperty(vertexPropertyColumnName, new StringType(value));
                                }
                            }
                        }
                    }

                    vertexList.add(vertex);
                } else {

                    LOG.warn("TRIBECA_WARN: Null vertex in " + columnName + ", row " + row.toString());
                    context.getCounter(GBHTableConfig.Counters.HTABLE_COLS_IGNORED).increment(1l);
                }
            }   // End of vertex block

            // check row for edges

            String propertyValue;
            String property;
            String srcVertexColName;
            String tgtVertexColName;

            for (String eLabel : edgeLabelList) {

                int          countEdgeAttr  = 0;
                EdgeRule     edgeRule           = edgeLabelToEdgeRules.get(eLabel);
                List<String> edgeAttributeList  = edgeRule.getPropertyColumnNames();
                String[]     edgeAttributes     = edgeAttributeList.toArray(new String[edgeAttributeList.size()]);

                // Get the src and tgt vertex ID's from GB_VidMap

                srcVertexColName     = edgeRule.getSrcColumnName();
                String srcVertexName = getColumnData(columns, srcVertexColName, context);

                tgtVertexColName     = edgeRule.getDstColumnName();
                String tgtVertexName = getColumnData(columns, tgtVertexColName, context);

                if (srcVertexColName != null && tgtVertexColName != null && eLabel != null) {
                    Edge<StringType> edge = new Edge<StringType>(new StringType(srcVertexName),
                                                                 new StringType(tgtVertexName),
                                                                 new StringType(eLabel));


                    // now add the edge properties

                    for (countEdgeAttr = 0; countEdgeAttr < edgeAttributes.length; countEdgeAttr++) {
                        propertyValue = getColumnData(columns, edgeAttributes[countEdgeAttr], context);


                        property = edgeAttributes[countEdgeAttr].replaceAll(
                                GBHTableConfig.config.getProperty("HBASE_COLUMN_SEPARATOR"),
                                GBHTableConfig.config.getProperty("TRIBECA_GRAPH_PROPERTY_SEPARATOR"));

                        if (property != null) {
                        edge.setProperty(property, new StringType(propertyValue));
                        }
                    }

                    edgeList.add(edge);

                    // need to make sure both ends of the edge are proper vertices!

                    Vertex<StringType> srcVertex = new Vertex<StringType>(new StringType(srcVertexName));
                    Vertex<StringType> tgtVertex = new Vertex<StringType>(new StringType(tgtVertexName));
                    vertexList.add(srcVertex);
                    vertexList.add(tgtVertex);

                    if (edgeRule.isBiDirectional()) {
                        Edge<StringType> opposingEdge = new Edge<StringType>(new StringType(tgtVertexName),
                                                            new StringType(srcVertexName),
                                                            new StringType(eLabel));

                        // now add the edge properties

                        for (countEdgeAttr = 0; countEdgeAttr < edgeAttributes.length; countEdgeAttr++) {
                            propertyValue = getColumnData(columns, edgeAttributes[countEdgeAttr], context);


                            property = edgeAttributes[countEdgeAttr].replaceAll(
                                    GBHTableConfig.config.getProperty("HBASE_COLUMN_SEPARATOR"),
                                    GBHTableConfig.config.getProperty("TRIBECA_GRAPH_PROPERTY_SEPARATOR"));

                            if (property != null) {
                                opposingEdge.setProperty(property, new StringType(propertyValue));
                            }
                        }
                        edgeList.add(opposingEdge);
                    }
                }

            }   // End of edge block

        } catch (Exception e) {
            e.printStackTrace();
            LOG.fatal("GRAPHBUILDER ERROR: " + e.toString());
            context.getCounter(GBHTableConfig.Counters.ERROR).increment(1);
        }
    }

    public Iterator<Vertex<StringType>> getVertices() {
        return vertexList.iterator();
    }

    @Override
    public Iterator<Edge<StringType>> getEdges() {
        return edgeList.iterator();
    }
}
