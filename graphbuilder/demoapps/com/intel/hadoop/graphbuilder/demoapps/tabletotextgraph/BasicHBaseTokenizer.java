

package com.intel.hadoop.graphbuilder.demoapps.tabletotextgraph;

import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema.VertexSchema;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.RecordTypeHBaseRow;
import com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.GBHTableConfig;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.CommandLineInterface;
import com.intel.hadoop.graphbuilder.util.HBaseUtils;
import com.thinkaurelius.titan.core.TitanKey;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BasicHBaseTokenizer implements GraphTokenizer<RecordTypeHBaseRow, StringType> {

    private static final Logger LOG = Logger.getLogger(BasicHBaseTokenizer.class);

    private List<String>                  vertexIdColumnList;
    private HashMap<String, String[]>     vertexPropColMap;
    private ArrayList<Vertex<StringType>> vertexList;

    private HashMap<String, List<String>> edgeLabelToColumnList;
    private ArrayList<String>             edgeLabelList;
    private ArrayList<Edge<StringType>>   edgeList;


    public BasicHBaseTokenizer() throws ParserConfigurationException {

        vertexPropColMap   = new HashMap<String, String[]>();
        vertexIdColumnList = new ArrayList<String>();
        vertexList         = new ArrayList<Vertex<StringType>>();

        edgeLabelToColumnList = new HashMap<String, List<String>>();
        edgeLabelList         = new ArrayList<String>();
        edgeList              = new ArrayList<Edge<StringType>>();
    }

    @Override
    public void configure(Configuration conf) {

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


        String[] edgeRules = BasicHBaseGraphBuildingRule.unpackEdgeRulesFromConfiguration(conf);

        for (String edgeRule : edgeRules) {

            String   srcVertexColName     = BasicHBaseGraphBuildingRule.getSrcColNameFromEdgeRule(edgeRule);
            String   tgtVertexColName     = BasicHBaseGraphBuildingRule.getDstColNameFromEdgeRule(edgeRule);
            String   label                = BasicHBaseGraphBuildingRule.getLabelFromEdgeRule(edgeRule);
            List<String> edgePropertyCols = BasicHBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(edgeRule);

            List<String> edgeColumns = new ArrayList<String>();

            //  edgeColumns tells us the columns used to generate edges of
            //  type "label"... "label" is used as the kep in the edge attribute list
            //  the source column is the first entry in the list, the destination column
            //  is the second, and the properties follow

            edgeColumns.add(srcVertexColName);
            edgeColumns.add(tgtVertexColName);

            for (String edgePropertyColumn : edgePropertyCols) {
                edgeColumns.add(edgePropertyColumn);
            }

            edgeLabelToColumnList.put(label, edgeColumns);
            edgeLabelList.add(label);
        }
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
                List<String> list           = edgeLabelToColumnList.get(eLabel);
                String[]     edgeAttributes = list.toArray(new String[list.size()]);

                // Get the src and tgt vertex ID's from GB_VidMap

                srcVertexColName     = edgeAttributes[0];
                String srcVertexName = getColumnData(columns, srcVertexColName, context);

                tgtVertexColName     = edgeAttributes[1];
                String tgtVertexName = getColumnData(columns, tgtVertexColName, context);

                if (srcVertexColName != null && tgtVertexColName != null && eLabel != null) {
                    Edge<StringType> edge = new Edge<StringType>(new StringType(srcVertexName),
                                                                 new StringType(tgtVertexName),
                                                                 new StringType(eLabel));

                    // now add the edge properties

                    for (countEdgeAttr = 2; countEdgeAttr < edgeAttributes.length; countEdgeAttr++) {
                        propertyValue = getColumnData(columns, edgeAttributes[countEdgeAttr], context);


                        property = edgeAttributes[countEdgeAttr].replaceAll(GBHTableConfig.config.getProperty("HBASE_COLUMN_SEPARATOR"),
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
