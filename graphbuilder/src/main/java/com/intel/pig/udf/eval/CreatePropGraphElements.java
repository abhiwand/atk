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

package com.intel.pig.udf.eval;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.types.*;
import com.intel.hadoop.graphbuilder.util.BaseCLI;
import com.intel.hadoop.graphbuilder.util.CommandLineInterface;
import com.intel.pig.data.GBTupleFactory;
import com.intel.pig.data.PropertyGraphElementTuple;
import com.intel.pig.udf.GBUdfException;
import com.intel.pig.udf.GBUdfExceptionHandler;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.tools.pigstats.PigStatusReporter;
import org.apache.commons.lang3.ArrayUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.TimeUnit;
/**
 * CreatePropGraphElements ... converts tuples of scalar data into bag of property graph elements..
 * <p/>
 *
 * <b>Example of use in pig script:</b>
 *
 * <pre>
 * REGISTER target/graphbuilder-2.0alpha-with-deps.jar;
 * x = LOAD 'examples/data/employees.csv' USING PigStorage(',') AS (id: int, name: chararray, age: int,
 * dept: chararray, managerId: int, tenure: chararray);
 * DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "name=age,managerId" -e "name,
 * dept,worksAt,tenure"');
 * pge = FOREACH x GENERATE flatten(CreatePropGraphElements(*));
 * </pre>
 *
 * The argument to the UDF constructor is a command string interpreted in the following manner:
 * The rules for specifying a graph are, at present, as follows:
 * </p>
 * <p/>
 * <p>
 * <p>EDGES:
 * The first three attributes in the edge string are source vertex field name, destination
 * vertex field name and the string label. Optional property values are listed by the field names by which they
 * are taken.</p>
 * <code> src_fname,dest_fname>,label,edge_property_fname1,...edge_property_fnamen </code>
 * </p>
 * <p>
 * <p>VERTICES: The first attribute in the string is an optional vertex label, the next is the required
 *  vertex ID field name. Subsequent attributes denote vertex properties
 * and are separated from the first by an equals sign:</p>
 * <code> vertex_id_fieldname=vertex_prop1_fieldname,... vertex_propn_fieldname</code>
 * <p>or in the case there are no properties associated with the vertex id:
 * <code> vertex_id_fieldname </code>
 * </p>
 *  * <p>
 *     EXAMPLE:
 *     <p>
 *<code>-v "name=age" -e "name,dept,worksAt,seniority"</code>
 *     </p>
 *     This generates a vertex for each employee annotated by their age, a vertex for each department with at least
 *     one employee, and an edge labeled "worksAt" between each employee and their department, annotated by their
 *     seniority in that department.
 * </p>
 * </p>
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class, duration = 30, timeUnit = TimeUnit.MINUTES)
public class CreatePropGraphElements extends EvalFunc<DataBag> {

    /**
     * Special vertex name used when retaining dangling edgess
     */
    public static final String NULL_VERTEX_NAME = "null";

    private CommandLineInterface commandLineInterface = new CommandLineInterface();

    private BagFactory mBagFactory = BagFactory.getInstance();

    private boolean retainDanglingEdges = false;
    private boolean flattenLists = false;
    private List<String> vertexIdFieldList;
    private Hashtable<String, String[]> vertexPropToFieldNamesMap;
    private Hashtable<String, String> vertexLabelMap;
    private Hashtable<String, EdgeRule> edgeLabelToEdgeRules;

    /**
     * Implements the dangling edge counters.
     * Use getter methods to call these counters
     */
    public enum Counters {
        NUM_DANGLING_EDGES,
        NUM_EDGES,
        NUM_VERTICES
    }

    /**
     * Encapsulation of the rules for creating edges.
     *
     * <p> Edge rules consist of the following:
     * <ul>
     * <li> A field name from which to read the edge's source vertex</li>
     * <li> A field name from which to read the edge's destination vertex</li>
     * <li> A boolean flag denoting if the edge is bidirectional or directed</li>
     * <li> A list of field names from which to read the edge's properties</li>
     * </ul></p>
     * <p>Edge rules are indexed by their label, so we do not store the label in the rule.</p>
     */
    private class EdgeRule {
        private String       srcFieldName;
        private String       dstFieldName;
        private List<String> propertyFieldNames;
        boolean              isBiDirectional;

        private EdgeRule() {

        }

        /**
         * Constructor must take source, destination and bidirectionality as arguments.
         * <p>There is no public default constructor.</p>
         * @param srcFieldName  column name from which to get source vertex
         * @param dstFieldName  column name from which to get destination vertex
         * @param biDirectional  is this edge bidirectional or not?
         */
        EdgeRule(String srcFieldName, String dstFieldName, boolean biDirectional) {
            this.srcFieldName = srcFieldName;
            this.dstFieldName = dstFieldName;
            this.propertyFieldNames = new ArrayList<String>();
            this.isBiDirectional     = biDirectional;
        }

        String getSrcFieldName() {
            return this.srcFieldName;
        }

        String getDstFieldName() {
            return this.dstFieldName;
        }

        boolean isBiDirectional() {
            return this.isBiDirectional;
        }

        void addPropertyColumnName(String columnName) {
            propertyFieldNames.add(columnName);
        }

        List<String> getPropertyFieldNames() {
            return propertyFieldNames;
        }
    }

    final boolean BIDIRECTIONAL = true;
    final boolean DIRECTED      = false;

    /*
     * A helper function that replaces nulls with empty lists.
     */
    private String[] nullIntoEmptyArray(String[] in) {
        if (in == null) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        } else {
            return in;
        }
    }

    /**
     * UDF Constructor : uses parse rule in command string to initialize graph construction rules.
     *
     * @param tokenizationRule
     */
    public CreatePropGraphElements(String tokenizationRule) {

        CommandLineInterface commandLineInterface = new CommandLineInterface();

        Options options = new Options();
        options.addOption(BaseCLI.Options.vertex.get());
        options.addOption(BaseCLI.Options.edge.get());
        options.addOption(BaseCLI.Options.directedEdge.get());
        options.addOption(BaseCLI.Options.flattenList.get());
        options.addOption(BaseCLI.Options.retainDanglingEdges.get());

        commandLineInterface.setOptions(options);

        CommandLine cmd = commandLineInterface.checkCli(tokenizationRule.split(" "));

        vertexLabelMap            = new Hashtable<String, String>();
        vertexPropToFieldNamesMap = new Hashtable<String, String[]>();
        vertexIdFieldList         = new ArrayList<String>();
        edgeLabelToEdgeRules      = new Hashtable<String, EdgeRule>();

        String[] vertexRules  = nullIntoEmptyArray(cmd.getOptionValues(BaseCLI.Options.vertex.getLongOpt()));
        String[] rawEdgeRules = nullIntoEmptyArray(cmd.getOptionValues(BaseCLI.Options.edge.getLongOpt()));
        String[] rawDirectedEdgeRules = nullIntoEmptyArray(cmd.getOptionValues(BaseCLI.Options.directedEdge
                .getLongOpt()));

        flattenLists = cmd.hasOption(BaseCLI.Options.flattenList.getLongOpt());
        retainDanglingEdges = cmd.hasOption(BaseCLI.Options.retainDanglingEdges.getLongOpt());

        // Parse the column names of vertices and properties from command line prompt
        // <vertex_col1>=[<vertex_prop1>,...] [<vertex_col2>=[<vertex_prop1>,...]]


        String   vertexIdFieldName  = null;
        String   vertexLabel      = null;

        for (String vertexRule : vertexRules) {

            // this tokenizer is based off the old HBase -> graph tokenizer and uses those parsing/extraction
            // routines as subroutines... those routines have nothing to do with hbase and simply extract field
            // ("column") names from command strings

            vertexIdFieldName = HBaseGraphBuildingRule.getVidColNameFromVertexRule(vertexRule);

            vertexIdFieldList.add(vertexIdFieldName);

            String[] vertexPropertiesFieldNames =
                    HBaseGraphBuildingRule.getVertexPropertyColumnsFromVertexRule(vertexRule);

            vertexPropToFieldNamesMap.put(vertexIdFieldName, vertexPropertiesFieldNames);

            // Vertex labels are maintained in a separate map
            vertexLabel = HBaseGraphBuildingRule.getRDFTagFromVertexRule(vertexRule);
            if (vertexLabel != null) {
                vertexLabelMap.put(vertexIdFieldName, vertexLabel);
            }
        }

        for (String rawEdgeRule : rawEdgeRules) {

            String   srcVertexFieldName     = HBaseGraphBuildingRule.getSrcColNameFromEdgeRule(rawEdgeRule);
            String   tgtVertexFieldName     = HBaseGraphBuildingRule.getDstColNameFromEdgeRule(rawEdgeRule);
            String   label                  = HBaseGraphBuildingRule.getLabelFromEdgeRule(rawEdgeRule);
            List<String> edgePropertyFieldNames =
                    HBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(rawEdgeRule);

            EdgeRule edgeRule = new EdgeRule(srcVertexFieldName, tgtVertexFieldName, BIDIRECTIONAL);

            for (String edgePropertyFieldName : edgePropertyFieldNames) {
                edgeRule.addPropertyColumnName(edgePropertyFieldName);
            }
            edgeLabelToEdgeRules.put(label, edgeRule);
        }

        for (String rawDirectedEdgeRule : rawDirectedEdgeRules) {

            String   srcVertexFieldName     = HBaseGraphBuildingRule.getSrcColNameFromEdgeRule(rawDirectedEdgeRule);
            String   tgtVertexFieldName     = HBaseGraphBuildingRule.getDstColNameFromEdgeRule(rawDirectedEdgeRule);
            String   label                  = HBaseGraphBuildingRule.getLabelFromEdgeRule(rawDirectedEdgeRule);
            List<String> edgePropertyFieldNames =
                    HBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(rawDirectedEdgeRule);

            EdgeRule edgeRule = new EdgeRule(srcVertexFieldName, tgtVertexFieldName, DIRECTED);

            for (String edgePropertyFieldName : edgePropertyFieldNames) {
                edgeRule.addPropertyColumnName(edgePropertyFieldName);
            }

            edgeLabelToEdgeRules.put(label, edgeRule);
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
            outArray = new String[] {string} ;
        }

        return outArray;
    }

    /**
     * Get an element from a tuple
     * @param input to get field from
     * @param inputSchema schema to determine what index to read from tuple
     * @param fieldName name of field to get from tuple
     * @return an element from the tuple
     */
    private Object getTupleData(Tuple input, Schema inputSchema, String fieldName) throws IOException{

        int fieldPos = inputSchema.getPosition(fieldName);
        if (fieldPos < 0) {
            throw new IllegalArgumentException("Did NOT find field named: " + fieldName + " in input schema");
        }
        return input.get(fieldPos);
    }

    private void addVertexToPropElementBag(DataBag outputBag, Vertex vertex) throws IOException {

        PropertyGraphElementTuple graphElementTuple = (PropertyGraphElementTuple) new GBTupleFactory()
                .newTuple(1);

        SerializedGraphElementStringTypeVids serializedgraphElement = new SerializedGraphElementStringTypeVids();

        serializedgraphElement.init(vertex);

        try {
            graphElementTuple.set(0, serializedgraphElement);
            outputBag.add(graphElementTuple);
            incrementCounter(Counters.NUM_VERTICES, 1L);
        } catch (ExecException e) {
            warn("Could not set output tuple", PigWarning.UDF_WARNING_1);
            throw new IOException(new GBUdfException(e));
        }
    }

    private void addEdgeToPropElementBag(DataBag outputBag, Edge edge) throws IOException{

        PropertyGraphElementTuple graphElementTuple = (PropertyGraphElementTuple) new GBTupleFactory().newTuple(1);

        SerializedGraphElementStringTypeVids serializedGraphElement = new SerializedGraphElementStringTypeVids();

        serializedGraphElement.init(edge);

        try {
            graphElementTuple.set(0, serializedGraphElement);
            outputBag.add(graphElementTuple);
        } catch (ExecException e) {
            warn("Could not set output tuple", PigWarning.UDF_WARNING_1);
            throw new IOException(new GBUdfException(e));
        }
    }

    /**
     * Converts Pig serialized data types to Java data types
     * BYTE -> Integer
     * Integer -> Integer
     * Long -> Long
     * Float -> Float
     * Double -> Double
     * Chararray -> String
     * @param value
     * @param typeByte
     * @return
     * @throws IllegalArgumentException
     */
    private WritableComparable pigTypesToSerializedJavaTypes(Object value, byte typeByte)
            throws IllegalArgumentException {

        WritableComparable object = null;

        switch(typeByte) {
            case DataType.BYTE:
                object = new IntType((Integer) value);
                break;
            case DataType.INTEGER:
                object = new IntType((Integer) value);
                break;
            case DataType.LONG:
                object = new LongType((Long) value);
                break;
            case DataType.FLOAT:
                object = new FloatType((Float) value);
                break;
            case DataType.DOUBLE:
                object = new DoubleType((Double) value);
                break;
            case DataType.CHARARRAY:
                object = new StringType((String) value);
                break;
            default:
                warn("Invalid data type", PigWarning.UDF_WARNING_1);
                throw new IllegalArgumentException();

        }

        return object;
    }

    /**
     * exec - the workhorse for the CreatePropGraphElements UDF
     *
     * Takes a tuple of scalars and outputs a bag of property graph elements.
     *
     * @param input a tuple of scalars
     * @return bag of property graph elements
     * @throws IOException
     */
    @Override
    public DataBag exec(Tuple input) throws IOException {

        Schema inputSchema = getInputSchema();
        Hashtable<String, Byte> fieldNameToDataType = new Hashtable<String,Byte>();

        for (Schema.FieldSchema field : inputSchema.getFields()) {
            fieldNameToDataType.put(field.alias, field.type);
        }

        DataBag outputBag = mBagFactory.newDefaultBag();

        // check tuple for vertices

        for (String fieldName : vertexIdFieldList) {

            String vidCell = (String) getTupleData(input, inputSchema, fieldName);

            if (null != vidCell) {

                for (String vertexId : expandString(vidCell)) {

                    // create vertex

                    Vertex<StringType> vertex = new Vertex<StringType>(new StringType(vertexId));

                    // add the vertex properties

                    String[] vpFieldNames = vertexPropToFieldNamesMap.get(fieldName);

                    if (null != vpFieldNames && vpFieldNames.length > 0) {
                        for (String vertexPropertyFieldName : vpFieldNames) {
                            Object value = null;

                            value =  getTupleData(input, inputSchema, vertexPropertyFieldName);
                            if (value != null) {
                                try {
                                    vertex.setProperty(vertexPropertyFieldName, pigTypesToSerializedJavaTypes(value,
                                            fieldNameToDataType.get(vertexPropertyFieldName)));
                                } catch (ClassCastException e) {
                                    warn("Cannot cast Pig type to Java type, skipping entry.", PigWarning.UDF_WARNING_1);
                                }
                            }
                        }
                    }

                    // add the abel to the vertex

                    String label = vertexLabelMap.get(fieldName);
                    if (label != null) {
                        vertex.setLabel(new StringType(label));
                    }
                    addVertexToPropElementBag(outputBag, vertex);
                }
            }  else {
                warn("Null data, skipping tuple.", PigWarning.UDF_WARNING_1);
            }
        }   // End of vertex block

        // check tuple for edges

        String srcVertexFieldName;
        String tgtVertexFieldName;

        for (String eLabel : edgeLabelToEdgeRules.keySet()) {

            EdgeRule     edgeRule           = edgeLabelToEdgeRules.get(eLabel);
            List<String> edgeAttributeList  = edgeRule.getPropertyFieldNames();
            String[]     edgeAttributes     = edgeAttributeList.toArray(new String[edgeAttributeList.size()]);

            srcVertexFieldName     = edgeRule.getSrcFieldName();
            tgtVertexFieldName     = edgeRule.getDstFieldName();

            String srcVertexCellString = (String) getTupleData(input, inputSchema, srcVertexFieldName);
            String tgtVertexCellString = (String) getTupleData(input, inputSchema, tgtVertexFieldName);

            StringType srcLabel = null;
            String srcLabelString = vertexLabelMap.get(srcVertexFieldName);
            if (srcLabelString != null) {
                srcLabel = new StringType(srcLabelString);
            }

            StringType tgtLabel = null;
            String tgtLabelString = vertexLabelMap.get(tgtVertexFieldName);
            if (tgtLabelString != null) {
                tgtLabel = new StringType(tgtLabelString);
            }

            processEdges(input, inputSchema, srcVertexCellString, tgtVertexCellString, srcLabel, tgtLabel, eLabel,
                    edgeAttributes, edgeRule, fieldNameToDataType, outputBag);
        }

        return outputBag;
    }

    /**
     * Provide return type information back to the Pig level.
     * @param input
     * @return Schema for a bag of property graph elements packed into unary tuples.
     */
    @Override
    public Schema outputSchema(Schema input) {
        try {

            Schema pgeTuple = new Schema(new Schema.FieldSchema("property_graph_element_schema", DataType.TUPLE));
            return new Schema(new Schema.FieldSchema(null, pgeTuple, DataType.BAG));

        } catch (FrontendException e) {
            // This should not happen
            throw new RuntimeException("Bug : exception thrown while "
                    + "creating output schema for CreatePropGraphElements udf", e);
        }
    }

    /**
     * Increment counters for Hadoop Job in this UDF
     * @param key   Enumerate counter name
     * @param value Increment value
     */
    public void incrementCounter(Enum key, Long value) {

        PigStatusReporter reporter = PigStatusReporter.getInstance();
        Counter reporterCounter = reporter.getCounter(key);
        if (reporter != null && reporterCounter != null) {
            reporterCounter.increment(value);
        }
    }

    /**
     * This function creates edges from two source and target data structures. These data structures can be either
     * primitive Java data types or comma-separated strings enclosed in curly braces
     *
     * @param input
     * @param inputSchema
     * @param srcVertexCellString
     * @param tgtVertexCellString
     * @param srcLabel
     * @param tgtLabel
     * @param eLabel
     * @param edgeAttributes
     * @param edgeRule
     * @param fieldNameToDataType
     * @param outputBag
     * @throws IOException
     */
    public void processEdges(Tuple input, Schema inputSchema, String srcVertexCellString, String tgtVertexCellString,
                            StringType srcLabel, StringType tgtLabel, String eLabel, String[] edgeAttributes,
                            EdgeRule edgeRule, Hashtable<String, Byte> fieldNameToDataType,
                            DataBag outputBag) throws IOException {

        List<String> srcVertexList = parseVertexCommaSeparatedList(srcVertexCellString);
        List<String> tgtVertexList = parseVertexCommaSeparatedList(tgtVertexCellString);

        for(String srcVertex : srcVertexList) {
            for(String tgtVertex : tgtVertexList) {

                // skip case where both are null
                if (srcVertex.equals(NULL_VERTEX_NAME) && tgtVertex.equals(NULL_VERTEX_NAME)) {
                    continue;
                }

                createEdge(input, inputSchema, srcVertex, tgtVertex, srcLabel, tgtLabel,
                        eLabel,  edgeAttributes,  edgeRule, fieldNameToDataType, outputBag);
            }

        }
    }   // End of processEdges() method definition

    /**
     * Parse a comma separated string and convert to a list of vertex names
     * @param vertexCellString comma separated list. E.g. a,b,,d
     * @return List of "a, b, null, d" or "a, b, d" depending on retainDanglingEdges
     */
    private List<String> parseVertexCommaSeparatedList(String vertexCellString) {
        List<String> list = new ArrayList<String>();

        if (vertexCellString == null) {
            if (retainDanglingEdges) {
                list.add(NULL_VERTEX_NAME);
            }
        }
        else {
            for (String vertexName: expandString(vertexCellString)) {
                if (vertexName.isEmpty() && retainDanglingEdges) {
                    list.add(NULL_VERTEX_NAME);
                }
                else {
                    list.add(vertexName);
                }
            }
        }
        return list;
    }

    /**
     * Function to add Edge, source and target vertices and edge properties to the outputBag
     * @throws IOException
     */
    private void createEdge(Tuple input, Schema inputSchema, String srcVertexName,
                             String tgtVertexName, StringType srcLabel, StringType tgtLabel,
                             String eLabel, String[] edgeAttributes, EdgeRule edgeRule,
                             Hashtable<String, Byte> fieldNameToDataType, DataBag outputBag) throws IOException {


        StringType currentSrcVertexName = new StringType(srcVertexName);
        StringType currentTgtVertexName = new StringType(tgtVertexName);

        addNewEdge(input, inputSchema, edgeAttributes, fieldNameToDataType, currentSrcVertexName, srcLabel,
                currentTgtVertexName, tgtLabel, eLabel, outputBag);


        // need to make sure both ends of the edge are proper
        // vertices!

        Vertex<StringType> srcVertex = new Vertex<StringType>(currentSrcVertexName, srcLabel);
        Vertex<StringType> tgtVertex = new Vertex<StringType>(currentTgtVertexName, tgtLabel);
        addVertexToPropElementBag(outputBag, srcVertex);
        addVertexToPropElementBag(outputBag, tgtVertex);

        if (edgeRule.isBiDirectional()) {

            addNewEdge(input, inputSchema, edgeAttributes, fieldNameToDataType, currentTgtVertexName, tgtLabel,
                    currentSrcVertexName, srcLabel, eLabel, outputBag);
        }
    }   // End of processEdges

    /**
     * Creates the edge object, adds the properties to it and adds the edge object to the output bag
     * @param input
     * @param inputSchema
     * @param edgeAttributes
     * @param fieldNameToDataType
     * @param srcVertexName
     * @param srcLabel
     * @param tgtVertexName
     * @param tgtLabel
     * @param eLabel
     * @param outputBag
     * @throws IOException
     */
    private void addNewEdge(Tuple input, Schema inputSchema, String[] edgeAttributes, Hashtable<String,
            Byte> fieldNameToDataType, StringType srcVertexName, StringType srcLabel,
                            StringType tgtVertexName, StringType tgtLabel, String eLabel,
                            DataBag outputBag) throws IOException {

        Edge<StringType> edge = new Edge<StringType>(srcVertexName,srcLabel, tgtVertexName, tgtLabel, new StringType(eLabel));

        String property = "";
        Object propertyValue = null;

        for (int countEdgeAttr = 0; countEdgeAttr < edgeAttributes.length; countEdgeAttr++) {
            propertyValue =  getTupleData(input, inputSchema, edgeAttributes[countEdgeAttr]);
            property = edgeAttributes[countEdgeAttr];

            if (propertyValue != null) {
                edge.setProperty(property, pigTypesToSerializedJavaTypes(propertyValue,
                        fieldNameToDataType.get(edgeAttributes[countEdgeAttr])));
            }
        }

        addEdgeToPropElementBag(outputBag, edge);
        incrementCounter(Counters.NUM_EDGES, 1L);

        if (isDangling(srcVertexName.get(), tgtVertexName.get()) && this.retainDanglingEdges) {
            incrementCounter(Counters.NUM_DANGLING_EDGES, 1L);
        }
    }

    /**
     *
     * @param srcVertexName
     * @param tgtVertexName
     * @return
     */
    private boolean isDangling(String srcVertexName, String tgtVertexName) {
        return srcVertexName.equals(NULL_VERTEX_NAME) || tgtVertexName.equals(NULL_VERTEX_NAME);
    }
}
