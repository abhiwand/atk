

package com.intel.hadoop.graphbuilder.demoapps.tabletotextgraph;

import com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.GBHTableConfig;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.RecordTypeHBaseRow;
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

import java.util.*;
import javax.xml.parsers.ParserConfigurationException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * MR-time routine that creates property graph elements from HBase rows.
 *
 * <p>Its set-up time analog is {@code BasicHBaseGraphBuildingRule} </p>
 *
 * @see BasicHBaseGraphBuildingRule
 * @see com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration.HBaseInputConfiguration
 * @see com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.HBaseReaderMapper
 */
public class BasicHBaseTokenizer implements GraphTokenizer<RecordTypeHBaseRow, StringType> {

    private static final Logger LOG = Logger.getLogger(BasicHBaseTokenizer.class);

    private List<String>                  vertexIdColumnList;
    private HashMap<String, String[]>     vertexPropColMap;
    private ArrayList<Vertex<StringType>> vertexList;

    private HashMap<String, EdgeRule>     edgeLabelToEdgeRules;
    private ArrayList<String>             edgeLabelList;
    private ArrayList<Edge<StringType>>   edgeList;

    private boolean                       flattenLists;



    /**
     * Encapsulation of the rules for creating edges.
     *
     * <p> Edge rules consist of the following:
     * <ul>
     * <li> A column name from which to read the edge's source vertex</li>
     * <li> A column name from which to read the edge's destination vertex</li>
     * <li> A boolean flag denoting if the edge is bidirectional or directed</li>
     * <li> A list of column names from which to read the edge's properties</li>
     * </ul></p>
     * <p>Edge rules are indexed by their label, so we do not store the label in the rule.</p>
     */
    private class EdgeRule {
        private String       srcColumnName;
        private String       dstColumnName;
        private List<String> propertyColumnNames;
        boolean              isBiDirectional;

        private EdgeRule() {

        };

        /**
         * Constructor must take source, destination and bidirectionality as arguments.
         * <p>There is no public default constructor.</p>
         * @param srcColumnName  column name from which to get source vertex
         * @param dstColumnName  column name from which to get destination vertex
         * @param biDirectional  is this edge bidirectional or not?
         */
        EdgeRule(String srcColumnName, String dstColumnName, boolean biDirectional) {
            this.srcColumnName       = srcColumnName;
            this.dstColumnName       = dstColumnName;
            this.propertyColumnNames = new ArrayList<String>();
            this.isBiDirectional     = biDirectional;
        }

        String getSrcColumnName() {
            return this.srcColumnName;
        }

        String getDstColumnName() {
            return this.dstColumnName;
        }

        boolean isBiDirectional() {
            return this.isBiDirectional;
        }

        void addPropertyColumnName(String columnName) {
            propertyColumnNames.add(columnName);
        }

        List<String> getPropertyColumnNames() {
            return propertyColumnNames;
        }



    }

    /**
     * Allocates the tokenizer and its constituent collections.
     *
     */

    public BasicHBaseTokenizer() {

        vertexPropColMap   = new HashMap<String, String[]>();
        vertexIdColumnList = new ArrayList<String>();
        vertexList         = new ArrayList<Vertex<StringType>>();

        edgeLabelToEdgeRules  = new HashMap<String, EdgeRule>();
        edgeLabelList         = new ArrayList<String>();
        edgeList              = new ArrayList<Edge<StringType>>();
    }

    /**
     * Extracts the vertex and edge generation rules from the configuration.
     *
     * The edge and vertex rules are placed in the configuration by {@code BasicHBaseGraphBuildingRule}
     *
     * @param conf  jobc configuration, provided by Hadoop
     * @see BasicHBaseGraphBuildingRule
     */
    @Override
    public void configure(Configuration conf) {

        this.flattenLists = conf.getBoolean("HBASE_TOKENIZER_FLATTEN_LISTS",false);

        // Parse the column names of vertices and properties from command line prompt
        // <vertex_col1>=[<vertex_prop1>,...] [<vertex_col2>=[<vertex_prop1>,...]]

        String[] vertexRules = BasicHBaseGraphBuildingRule.unpackVertexRulesFromConfiguration(conf);

        String   vertexIdColumnName  = null;

        for (String vertexRule : vertexRules) {

                vertexIdColumnName = BasicHBaseGraphBuildingRule.getVidColNameFromVertexRule(vertexRule);
                vertexIdColumnList.add(vertexIdColumnName);

                String[] vertexPropertiesColumnNames =
                        BasicHBaseGraphBuildingRule.getVertexPropertyColumnsFromVertexRule(vertexRule);

                vertexPropColMap.put(vertexIdColumnName, vertexPropertiesColumnNames);
        }

        LOG.info("TRIBECA_INFO: Number of vertice rules to be read from HBase = " + vertexIdColumnList.size());


        String[] rawEdgeRules         = BasicHBaseGraphBuildingRule.unpackEdgeRulesFromConfiguration(conf);
        String[] rawDirectedEdgeRules = BasicHBaseGraphBuildingRule.unpackDirectedEdgeRulesFromConfiguration(conf);

        final boolean BIDIRECTIONAL = true;
        final boolean DIRECTED      = false;

        for (String rawEdgeRule : rawEdgeRules) {

            String   srcVertexColName     = BasicHBaseGraphBuildingRule.getSrcColNameFromEdgeRule(rawEdgeRule);
            String   tgtVertexColName     = BasicHBaseGraphBuildingRule.getDstColNameFromEdgeRule(rawEdgeRule);
            String   label                = BasicHBaseGraphBuildingRule.getLabelFromEdgeRule(rawEdgeRule);
            List<String> edgePropertyCols =
                    BasicHBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(rawEdgeRule);

           EdgeRule edgeRule = new EdgeRule(srcVertexColName, tgtVertexColName, BIDIRECTIONAL);

            for (String edgePropertyColumn : edgePropertyCols) {
                edgeRule.addPropertyColumnName(edgePropertyColumn);
            }
            edgeLabelToEdgeRules.put(label, edgeRule);
            edgeLabelList.add(label);
        }

        for (String rawDirectedEdgeRule : rawDirectedEdgeRules) {

            String   srcVertexColName     = BasicHBaseGraphBuildingRule.getSrcColNameFromEdgeRule(rawDirectedEdgeRule);
            String   tgtVertexColName     = BasicHBaseGraphBuildingRule.getDstColNameFromEdgeRule(rawDirectedEdgeRule);
            String   label                = BasicHBaseGraphBuildingRule.getLabelFromEdgeRule(rawDirectedEdgeRule);
            List<String> edgePropertyCols =
                    BasicHBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(rawDirectedEdgeRule);

            EdgeRule edgeRule         = new EdgeRule(srcVertexColName, tgtVertexColName, DIRECTED);

            for (String edgePropertyColumn : edgePropertyCols) {
                edgeRule.addPropertyColumnName(edgePropertyColumn);
            }

            edgeLabelToEdgeRules.put(label, edgeRule);
            edgeLabelList.add(label);

        }

    }


    /**
     * Get column data from the HBase table. If any errors are encountered, log them.
     *
     * @param columns        HTable columns for the current row
     * @param fullColumnName Name of the HTABLE column - column_family:column_qualifier
     * @param context        Hadoop's mapper context. Used for error logging.
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

    private String[] expandString(String string) {

        String[] outArray = null;

        int inLength = string.length();

        if (this.flattenLists && string.startsWith("{") && string.endsWith("}")) {

            String bracesStrippedString     = string.substring(1,inLength-1);
            String parenthesesDroppedString = bracesStrippedString.replace("(","").replace(")","");
            String[] expandedString         = parenthesesDroppedString.split("\\,");
            outArray                        = expandedString;

        }  else {
            outArray    = new String[1];
            outArray[0] = string;
        }

        return outArray;
    }


    /**
     * Read an hbase record, and generate vertices and edges according to the generation rules
     * previously extracted from the configuration.
     *
     * @param record  An hbase row.
     * @param context The mapper's context. Used for error logging.
     */

    public void parse(RecordTypeHBaseRow record, Mapper.Context context) {

        ImmutableBytesWritable row     = record.getRow();
        Result                 columns = record.getColumns();

        vertexList.clear();
        edgeList.clear();

        // check row for vertices

        for (String columnName : vertexIdColumnList) {

            String vidCell = getColumnData(columns, columnName, context);

            for (String vertexId : expandString(vidCell)) {

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
            }
        }// End of vertex block

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


            srcVertexColName     = edgeRule.getSrcColumnName();
            tgtVertexColName     = edgeRule.getDstColumnName();

            String srcVertexCellString = getColumnData(columns, srcVertexColName, context);
            String tgtVertexCellString = getColumnData(columns, tgtVertexColName, context);


            for (String srcVertexName : expandString(srcVertexCellString)) {
                for (String tgtVertexName: expandString(tgtVertexCellString)) {

                    if (srcVertexColName != null && tgtVertexColName != null && eLabel != null) {

                        Edge<StringType> edge = new Edge<StringType>(new StringType(srcVertexName),
                                new StringType(tgtVertexName), new StringType(eLabel));

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
                }
            }
        }
    }

    /**
     * Obtain iterator over the vertex list.
     *
     * @return  Iterator over the vertex list.
     */
    public Iterator<Vertex<StringType>> getVertices() {
        return vertexList.iterator();
    }

    /**
     * Obtain iterator over the edge list.
     * @return Iterator over the edge list.
     */
    @Override
    public Iterator<Edge<StringType>> getEdges() {
        return edgeList.iterator();
    }
}
