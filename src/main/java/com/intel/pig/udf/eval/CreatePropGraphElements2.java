package com.intel.pig.udf.eval;

/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder
 */

import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.GBHTableConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.types.*;
import com.intel.hadoop.graphbuilder.util.BaseCLI;
import com.intel.hadoop.graphbuilder.util.CommandLineInterface;
import com.intel.pig.data.GBTupleFactory;
import com.intel.pig.data.PropertyGraphElementTuple;
import com.intel.pig.udf.GBUdfExceptionHandler;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.PropertyGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * \brief some documentation
 *
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class)
public class CreatePropGraphElements2 extends EvalFunc<DataBag> {
    private CommandLineInterface commandLineInterface = new CommandLineInterface();

    private BagFactory mBagFactory = BagFactory.getInstance();

    private String   tokenizationRule;
    private String[] rawEdgeRules;
    private String[] vertexRules;
    private String[] rawDirectedEdgeRules;

    private boolean flattenLists = false;
    private List<String>                  vertexIdColumnList;
    private HashMap<String, String[]>     vertexPropColMap;
    private HashMap<String, String>       vertexRDFLabelMap;

    private HashMap<String, EdgeRule>     edgeLabelToEdgeRules;
    private ArrayList<String>             edgeLabelList;

    private HashMap<String, Byte> fieldNameToDataType;

    private boolean                       flattenList;
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
     * A helper function that replaces nulls with empty lists.
     */
    private String[] nullsIntoEmptyStringArrays(String[] in) {
        if (in == null) {
            return new String[0];
        } else {
            return in;
        }
    }
    public CreatePropGraphElements2(String tokenizationRule) {
        commandLineInterface = new CommandLineInterface();
        Options options = new Options();

        options.addOption(BaseCLI.Options.vertex.get());

        options.addOption(BaseCLI.Options.edge.get());

        options.addOption(BaseCLI.Options.directedEdge.get());
;
        commandLineInterface.setOptions(options);

        CommandLine cmd = commandLineInterface.checkCli(tokenizationRule.split(" "));

        this.tokenizationRule = tokenizationRule;


        vertexRDFLabelMap  = new HashMap<String, String>();
        vertexPropColMap   = new HashMap<String, String[]>();
        vertexIdColumnList = new ArrayList<String>();

        edgeLabelToEdgeRules  = new HashMap<String, EdgeRule>();
        edgeLabelList         = new ArrayList<String>();


        vertexRules =
                nullsIntoEmptyStringArrays(cmd.getOptionValues(BaseCLI.Options.vertex.getLongOpt()));

        rawEdgeRules =
                nullsIntoEmptyStringArrays(cmd.getOptionValues(BaseCLI.Options.edge.getLongOpt()));

        rawDirectedEdgeRules =
                nullsIntoEmptyStringArrays(cmd.getOptionValues(BaseCLI.Options.directedEdge.getLongOpt()));

        // Parse the column names of vertices and properties from command line prompt
        // <vertex_col1>=[<vertex_prop1>,...] [<vertex_col2>=[<vertex_prop1>,...]]


        String   vertexIdColumnName  = null;
        String   vertexRDFLabel      = null;

        for (String vertexRule : vertexRules) {

            vertexIdColumnName = HBaseGraphBuildingRule.getVidColNameFromVertexRule(vertexRule);

            vertexIdColumnList.add(vertexIdColumnName);

            String[] vertexPropertiesColumnNames =
                    HBaseGraphBuildingRule.getVertexPropertyColumnsFromVertexRule(vertexRule);

            vertexPropColMap.put(vertexIdColumnName, vertexPropertiesColumnNames);

            // Vertex RDF labels are maintained in a separate map
            vertexRDFLabel = HBaseGraphBuildingRule.getRDFTagFromVertexRule(vertexRule);
            if (vertexRDFLabel != null) {
                vertexRDFLabelMap.put(vertexIdColumnName, vertexRDFLabel);
            }
        }


        final boolean BIDIRECTIONAL = true;
        final boolean DIRECTED      = false;

        for (String rawEdgeRule : rawEdgeRules) {

            String   srcVertexColName     = HBaseGraphBuildingRule.getSrcColNameFromEdgeRule(rawEdgeRule);
            String   tgtVertexColName     = HBaseGraphBuildingRule.getDstColNameFromEdgeRule(rawEdgeRule);
            String   label                = HBaseGraphBuildingRule.getLabelFromEdgeRule(rawEdgeRule);
            List<String> edgePropertyCols =
                    HBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(rawEdgeRule);

            EdgeRule edgeRule = new EdgeRule(srcVertexColName, tgtVertexColName, BIDIRECTIONAL);

            for (String edgePropertyColumn : edgePropertyCols) {
                edgeRule.addPropertyColumnName(edgePropertyColumn);
            }
            edgeLabelToEdgeRules.put(label, edgeRule);
            edgeLabelList.add(label);
        }

        for (String rawDirectedEdgeRule : rawDirectedEdgeRules) {

            String   srcVertexColName     = HBaseGraphBuildingRule.getSrcColNameFromEdgeRule(rawDirectedEdgeRule);
            String   tgtVertexColName     = HBaseGraphBuildingRule.getDstColNameFromEdgeRule(rawDirectedEdgeRule);
            String   label                = HBaseGraphBuildingRule.getLabelFromEdgeRule(rawDirectedEdgeRule);
            List<String> edgePropertyCols =
                    HBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(rawDirectedEdgeRule);

            EdgeRule edgeRule         = new EdgeRule(srcVertexColName, tgtVertexColName, DIRECTED);

            for (String edgePropertyColumn : edgePropertyCols) {
                edgeRule.addPropertyColumnName(edgePropertyColumn);
            }

            edgeLabelToEdgeRules.put(label, edgeRule);
            edgeLabelList.add(label);

        }
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

    private Object getTupleData(Tuple input, Schema inputSchema, String fieldName) throws IOException{

        int fieldPos = inputSchema.getPosition(fieldName);
        Object output = input.get(fieldPos);

        return  output;
    }

    private void addVertexToPropElementBag(DataBag outputBag, Vertex vertex) {

        PropertyGraphElementTuple graphElementTuple = (PropertyGraphElementTuple) new GBTupleFactory()
                .newTuple(1);

        PropertyGraphElementStringTypeVids graphElement = new PropertyGraphElementStringTypeVids();

        graphElement.init(PropertyGraphElement.GraphElementType.VERTEX, vertex);

        try {
            graphElementTuple.set(0, graphElement);
            outputBag.add(graphElementTuple);
        } catch (ExecException e) {
            // log something here, however pig logs stuff
        }
    }

    private void addEdgeToPropElementBag(DataBag outputBag, Edge edge) {

        PropertyGraphElementTuple graphElementTuple = (PropertyGraphElementTuple) new GBTupleFactory().newTuple(1);

        PropertyGraphElementStringTypeVids graphElement = new PropertyGraphElementStringTypeVids();

        graphElement.init(PropertyGraphElement.GraphElementType.EDGE, edge);

        try {
            graphElementTuple.set(0, graphElement);
            outputBag.add(graphElementTuple);
        } catch (ExecException e) {
            // log something here, however pig logs stuff
        }

    }

    private WritableComparable dataConverter(Object value, byte typeByte) {
        WritableComparable object = null;

        if (typeByte == DataType.BYTE) {
            object = new IntType((int) value);
        } else if (typeByte == DataType.INTEGER) {
            object = new IntType((int) value);
        } else if (typeByte == DataType.LONG) {
            object = new LongType((long) value);
        } else if (typeByte == DataType.FLOAT) {
            object = new FloatType((float) value);
        } else if (typeByte == DataType.DOUBLE) {
            object = new DoubleType((double) value);
        } else if (typeByte == DataType.CHARARRAY) {
            object = new StringType((String) value);
        } else {
            // todo: failure/exception
        }

        return object;
    }
    private Class<?> pigTypeByteToJavaClass(byte typeByte) {

        Class<?> javaClass = Object.class;

        if (typeByte == DataType.BYTE) {
            return Byte.class;
        } else if (typeByte == DataType.INTEGER) {
            return Integer.class;
        } else if (typeByte == DataType.LONG) {
            return Long.class;
        } else if (typeByte == DataType.FLOAT) {
            return Float.class;
        } else if (typeByte == DataType.DOUBLE) {
            return Double.class;
        } else if (typeByte == DataType.CHARARRAY) {
            return String.class;
        }

        return javaClass;
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {

        Schema inputSchema = getInputSchema();
        fieldNameToDataType = new HashMap<String,Byte>();

        for (Schema.FieldSchema field : inputSchema.getFields()) {
            fieldNameToDataType.put(field.alias, field.type);
        }

        DataBag outputBag = mBagFactory.newDefaultBag();

        // check tuple for vertices

        for (String columnName : vertexIdColumnList) {

            String vidCell = (String) getTupleData(input, inputSchema, columnName);

            if (null != vidCell) {
                for (String vertexId : expandString(vidCell)) {

                    // create vertex

                    Vertex<StringType> vertex = new Vertex<StringType>(new StringType(vertexId));

                    // add the vertex properties

                    String[] vpColNames = vertexPropColMap.get(columnName);

                    if (null != vpColNames) {



                        if (vpColNames.length > 0) {
                            for (String vertexPropertyColumnName : vpColNames) {
                                Object value = null;

                                value =  getTupleData(input, inputSchema, vertexPropertyColumnName);
                                if (value != null) {
                                    vertex.setProperty(vertexPropertyColumnName, dataConverter(value,fieldNameToDataType.get(vertexPropertyColumnName) ));
                                }
                            }
                        }
                    }

                    // add the RDF label to the vertex

                    String rdfLabel = vertexRDFLabelMap.get(columnName);
                    if (rdfLabel != null) {
                        vertex.setVertexLabel(new StringType(rdfLabel));
                    }
                    addVertexToPropElementBag(outputBag, vertex);
                }
            }  else {
                // what kind of logging and what error counting can we do with pig?
                    /*
                    LOG.warn("GRAPHBUILDER_WARN: Null vertex in " + columnName + ", row " + row.toString());
                    context.getCounter(GBHTableConfiguration.Counters.HTABLE_COLS_IGNORED).increment(1l);
                    */
            }
        }// End of vertex block

        // check row for edges

        Object propertyValue;
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

            String srcVertexCellString = (String) getTupleData(input, inputSchema, srcVertexColName);
            String tgtVertexCellString = (String) getTupleData(input, inputSchema, tgtVertexColName);

            if (srcVertexCellString != null && tgtVertexCellString != null && eLabel != null) {

                for (String srcVertexName : expandString(srcVertexCellString)) {
                    for (String tgtVertexName: expandString(tgtVertexCellString)) {

                        Edge<StringType> edge = new Edge<StringType>(new StringType(srcVertexName),
                                new StringType(tgtVertexName), new StringType(eLabel));

                        for (countEdgeAttr = 0; countEdgeAttr < edgeAttributes.length; countEdgeAttr++) {
                            propertyValue =  getTupleData(input, inputSchema, edgeAttributes[countEdgeAttr]);
                            property = edgeAttributes[countEdgeAttr];

                            if (propertyValue != null) {
                                edge.setProperty(property, dataConverter(propertyValue, fieldNameToDataType.get(edgeAttributes[countEdgeAttr]) ));
                            }
                        }

                        addEdgeToPropElementBag(outputBag, edge);

                        // need to make sure both ends of the edge are proper vertices!

                        Vertex<StringType> srcVertex = new Vertex<StringType>(new StringType(srcVertexName));
                        Vertex<StringType> tgtVertex = new Vertex<StringType>(new StringType(tgtVertexName));
                        addVertexToPropElementBag(outputBag, srcVertex);
                        addVertexToPropElementBag(outputBag, tgtVertex);

                        if (edgeRule.isBiDirectional()) {
                            Edge<StringType> opposingEdge = new Edge<StringType>(new StringType(tgtVertexName),
                                    new StringType(srcVertexName),
                                    new StringType(eLabel));

                            // now add the edge properties

                            for (countEdgeAttr = 0; countEdgeAttr < edgeAttributes.length; countEdgeAttr++) {
                                propertyValue = (String) getTupleData(input, inputSchema, edgeAttributes[countEdgeAttr]);

                                property = edgeAttributes[countEdgeAttr];

                                if (propertyValue != null) {
                                    edge.setProperty(property, dataConverter(propertyValue, fieldNameToDataType.get(edgeAttributes[countEdgeAttr]) ));
                                }
                            }
                            addEdgeToPropElementBag(outputBag, opposingEdge);
                        }
                    }
                }
            }
        }

        return outputBag;
    }

}
