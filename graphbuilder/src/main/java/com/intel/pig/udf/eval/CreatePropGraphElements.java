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
import com.intel.hadoop.graphbuilder.util.MultiValuedMap;
import com.intel.pig.data.GBTupleFactory;
import com.intel.pig.data.PropertyGraphElementTuple;
import com.intel.pig.rules.EdgeRule;
import com.intel.pig.udf.GBUdfException;
import com.intel.pig.udf.GBUdfExceptionHandler;
import com.intel.pig.udf.util.InputTupleInProgress;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.ArrayUtils;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * CreatePropGraphElements ... converts tuples of scalar data into bag of property graph elements..
 * <p/>
 * <p/>
 * <b>Example of use in pig script:</b>
 * <p/>
 * <pre>
 * REGISTER target/graphbuilder-2.0alpha-with-deps.jar;
 * x = LOAD 'examples/data/employees.csv' USING PigStorage(',') AS (id: int, name: chararray, age: int,
 * dept: chararray, managerId: int, tenure: chararray);
 * DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "name=age,managerId" -e "name,
 * dept,worksAt,tenure"');
 * pge = FOREACH x GENERATE flatten(CreatePropGraphElements(*));
 * </pre>
 * <p/>
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
 * vertex ID field name. Subsequent attributes denote vertex properties
 * and are separated from the first by an equals sign:</p>
 * <code> vertex_id_fieldname=vertex_prop1_fieldname,... vertex_propn_fieldname</code>
 * <p>or in the case there are no properties associated with the vertex id:
 * <code> vertex_id_fieldname </code>
 * </p>
 * * <p>
 * EXAMPLE:
 * <p>
 * <code>-v "name=age" -e "name,dept,worksAt,seniority"</code>
 * </p>
 * This generates a vertex for each employee annotated by their age, a vertex for each department with at least
 * one employee, and an edge labeled "worksAt" between each employee and their department, annotated by their
 * seniority in that department.
 * </p>
 * This generates a vertex for each employee annotated by their age, a vertex for each department with at least
 * one employee, and an edge labeled "worksAt" between each employee and their department, annotated by their
 * seniority in that department.
 * </p>
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class, duration = 30, timeUnit = TimeUnit.MINUTES)
public class CreatePropGraphElements extends EvalFunc<DataBag> {

    /**
     * Special vertex name used when retaining dangling edges
     */
    public static final String NULL_VERTEX_NAME = "null";
    public static final String VERTEX_PROPERTY_SIDE = "side";
    public static final String LEFT = "L";
    public static final String RIGHT = "R";

    final boolean BIDIRECTIONAL = true;
    final boolean DIRECTED = false;

    private boolean retainDanglingEdges = false;
    private boolean flattenLists = false;

    private BagFactory mBagFactory = BagFactory.getInstance();

    private List<String> vertexIdFieldList;

    private Hashtable<String, String[]> vertexPropToFieldNamesMap;
    private Hashtable<String, String> vertexLabelMap;
    private MultiValuedMap<String, EdgeRule> edgeLabelToEdgeRules;
    private boolean addSideToVertices = false;

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
        options.addOption(BaseCLI.Options.addSideToVertex.get());

        commandLineInterface.setOptions(options);

        CommandLine cmd = commandLineInterface.checkCli(tokenizationRule.split(" "));

        vertexLabelMap = new Hashtable<String, String>();
        vertexPropToFieldNamesMap = new Hashtable<String, String[]>();
        vertexIdFieldList = new ArrayList<String>();
        edgeLabelToEdgeRules = new MultiValuedMap<String, EdgeRule>();

        String[] vertexRules = nullIntoEmptyArray(cmd.getOptionValues(BaseCLI.Options.vertex.getLongOpt()));
        String[] rawEdgeRules = nullIntoEmptyArray(cmd.getOptionValues(BaseCLI.Options.edge.getLongOpt()));
        String[] rawDirectedEdgeRules = nullIntoEmptyArray(cmd.getOptionValues(BaseCLI.Options.directedEdge
                .getLongOpt()));

        flattenLists = cmd.hasOption(BaseCLI.Options.flattenList.getLongOpt());
        retainDanglingEdges = cmd.hasOption(BaseCLI.Options.retainDanglingEdges.getLongOpt());
        addSideToVertices = cmd.hasOption(BaseCLI.Options.addSideToVertex.getLongOpt());

        // Parse the column names of vertices and properties from command line prompt
        // <vertex_col1>=[<vertex_prop1>,...] [<vertex_col2>=[<vertex_prop1>,...]]

        for (String vertexRule : vertexRules) {

            // this tokenizer is based off the old HBase -> graph tokenizer and uses those parsing/extraction
            // routines as subroutines... those routines have nothing to do with hbase and simply extract field
            // ("column") names from command strings

            String vertexIdFieldName = HBaseGraphBuildingRule.getVidColNameFromVertexRule(vertexRule);

            vertexIdFieldList.add(vertexIdFieldName);

            String[] vertexPropertiesFieldNames =
                    HBaseGraphBuildingRule.getVertexPropertyColumnsFromVertexRule(vertexRule);

            vertexPropToFieldNamesMap.put(vertexIdFieldName, vertexPropertiesFieldNames);

            // Vertex labels are maintained in a separate map
            String vertexLabel = HBaseGraphBuildingRule.getRDFTagFromVertexRule(vertexRule);
            if (vertexLabel != null) {
                vertexLabelMap.put(vertexIdFieldName, vertexLabel);
            }
        }

        for (String rawEdgeRule : rawEdgeRules) {

            String srcVertexFieldName = HBaseGraphBuildingRule.getSrcColNameFromEdgeRule(rawEdgeRule);
            String tgtVertexFieldName = HBaseGraphBuildingRule.getDstColNameFromEdgeRule(rawEdgeRule);
            String label = HBaseGraphBuildingRule.getLabelFromEdgeRule(rawEdgeRule);
            List<String> edgePropertyFieldNames =
                    HBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(rawEdgeRule);

            EdgeRule edgeRule = new EdgeRule(srcVertexFieldName, tgtVertexFieldName, BIDIRECTIONAL, label,
                    edgePropertyFieldNames);

            edgeLabelToEdgeRules.addKeyValue(label, edgeRule);
        }

        for (String rawDirectedEdgeRule : rawDirectedEdgeRules) {

            String srcVertexFieldName = HBaseGraphBuildingRule.getSrcColNameFromEdgeRule(rawDirectedEdgeRule);
            String tgtVertexFieldName = HBaseGraphBuildingRule.getDstColNameFromEdgeRule(rawDirectedEdgeRule);
            String label = HBaseGraphBuildingRule.getLabelFromEdgeRule(rawDirectedEdgeRule);
            List<String> edgePropertyFieldNames =
                    HBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(rawDirectedEdgeRule);

            EdgeRule edgeRule = new EdgeRule(srcVertexFieldName, tgtVertexFieldName, DIRECTED, label,
                    edgePropertyFieldNames);

            edgeLabelToEdgeRules.addKeyValue(label, edgeRule);
        }
    }

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

    private String[] expandString(String string) {

        String[] outArray;

        int inLength = string.length();

        if (this.flattenLists && string.startsWith("{") && string.endsWith("}")) {

            String bracesStrippedString = string.substring(1, inLength - 1);
            String parenthesesDroppedString = bracesStrippedString.replace("(", "").replace(")", "");
            outArray = parenthesesDroppedString.split("\\,");
        } else {
            outArray = new String[]{string};
        }

        return outArray;
    }

    private void addVertexToPropElementBag(DataBag outputBag, Vertex vertex) throws IOException {

        PropertyGraphElementTuple graphElementTuple = (PropertyGraphElementTuple) new GBTupleFactory().newTuple(1);
        SerializedGraphElementStringTypeVids serializedGraphElement = new SerializedGraphElementStringTypeVids();
        serializedGraphElement.init(vertex);

        try {
            graphElementTuple.set(0, serializedGraphElement);
            outputBag.add(graphElementTuple);
            incrementCounter(Counters.NUM_VERTICES, 1L);
        } catch (ExecException e) {
            warn("Could not set output tuple", PigWarning.UDF_WARNING_1);
            throw new IOException(new GBUdfException(e));
        }
    }

    private void addEdgeToPropElementBag(DataBag outputBag, Edge edge) throws IOException {

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
     *
     * @param value
     * @param typeByte
     * @return
     * @throws IllegalArgumentException
     */
    private WritableComparable pigTypesToSerializedJavaTypes(Object value, byte typeByte)
            throws IllegalArgumentException {

        WritableComparable object;

        switch (typeByte) {
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
     * <p/>
     * Takes a tuple of scalars and outputs a bag of property graph elements.
     *
     * @param input a tuple of scalars
     * @return bag of property graph elements
     * @throws IOException
     */
    @Override
    public DataBag exec(Tuple input) throws IOException {

        InputTupleInProgress inputTupleInProgress = new InputTupleInProgress(input, getInputSchema());
        DataBag outputBag = mBagFactory.newDefaultBag();

        // Check input tuple for vertices

        extractVertices(inputTupleInProgress, outputBag);

        // Check input tuple for edges

        extractEdges(inputTupleInProgress, outputBag);

        return outputBag;
    }

    /**
     * Scan the vertex rules, create serialized Vertex elements and add them to the output Bag
     *
     * @param inputTupleInProgress
     * @param outputBag
     * @throws IOException
     */
    private void extractVertices(InputTupleInProgress inputTupleInProgress, DataBag outputBag) throws IOException {

        for (String fieldName : this.vertexIdFieldList) {

            Object vidCell = inputTupleInProgress.getTupleData(fieldName);

            if (vidCell == null) {
                warn("Null data, skipping tuple.", PigWarning.UDF_WARNING_1);
                continue;
            }

            for (String vertexId : expandString(vidCell.toString())) {

                Vertex<StringType> vertex = new Vertex<StringType>(new StringType(vertexId));

                // add the vertex properties

                String[] vpFieldNames = this.vertexPropToFieldNamesMap.get(fieldName);

                if (null != vpFieldNames && vpFieldNames.length > 0) {
                    for (String vertexPropertyFieldName : vpFieldNames) {
                        Object value = inputTupleInProgress.getTupleData(vertexPropertyFieldName);
                        if (value != null) {
                            try {
                                vertex.setProperty(vertexPropertyFieldName, pigTypesToSerializedJavaTypes(value,
                                        inputTupleInProgress.getType(vertexPropertyFieldName)));
                            } catch (ClassCastException e) {
                                warn("Cannot cast Pig type to Java type, skipping entry.", PigWarning.UDF_WARNING_1);
                            }
                        }
                    }
                }

                // add the label to the vertex

                String label = this.vertexLabelMap.get(fieldName);
                if (label != null) {
                    vertex.setLabel(new StringType(label));
                }
                addVertexToPropElementBag(outputBag, vertex);
            }
        }
    }

    /**
     * Scan the edge rules, create serialized Edge elements and add them to the output Bag
     *
     * @param inputTupleInProgress
     * @param outputBag
     * @throws IOException
     */
    private void extractEdges(InputTupleInProgress inputTupleInProgress, DataBag outputBag) throws IOException {

        for (String edgeLabel : this.edgeLabelToEdgeRules.keySet()) {

            Set<EdgeRule> edgeRules = this.edgeLabelToEdgeRules.getValues(edgeLabel);

            for (EdgeRule edgeRule : edgeRules) {
                String srcVertexFieldName = edgeRule.getSrcFieldName();
                String tgtVertexFieldName = edgeRule.getDstFieldName();

                Object srcVertexCell = inputTupleInProgress.getTupleData(srcVertexFieldName);
                Object tgtVertexCell = inputTupleInProgress.getTupleData(tgtVertexFieldName);

                if (srcVertexCell != null && tgtVertexCell != null) {

                    String srcVertexCellString = srcVertexCell.toString();
                    String tgtVertexCellString = tgtVertexCell.toString();

                    StringType srcLabel = null;
                    StringType tgtLabel = null;

                    String srcLabelString = this.vertexLabelMap.get(srcVertexFieldName);
                    if (srcLabelString != null) {
                        srcLabel = new StringType(srcLabelString);
                    }

                    String tgtLabelString = this.vertexLabelMap.get(tgtVertexFieldName);
                    if (tgtLabelString != null) {
                        tgtLabel = new StringType(tgtLabelString);
                    }

                    List<String> srcVertexList = parseCommaSeparatedVertexList(srcVertexCellString);
                    List<String> tgtVertexList = parseCommaSeparatedVertexList(tgtVertexCellString);

                    for (String srcVertex : srcVertexList) {
                        for (String tgtVertex : tgtVertexList) {

                            // skip case where both are null
                            if (srcVertex.equals(NULL_VERTEX_NAME) && tgtVertex.equals(NULL_VERTEX_NAME)) {
                                continue;
                            }

                            createEdge(inputTupleInProgress, srcVertex, tgtVertex, srcLabel, tgtLabel, edgeRule, outputBag);
                        }
                    }
                }
            }
        }
    }

    /**
     * Function to add Edge, source and target vertices and edge properties to the outputBag
     *
     * @param inputTupleInProgress
     * @param srcVertexName
     * @param tgtVertexName
     * @param srcLabel
     * @param tgtLabel
     * @param edgeRule
     * @param outputBag
     * @throws IOException
     */
    private void createEdge(InputTupleInProgress inputTupleInProgress, String srcVertexName,
                            String tgtVertexName, StringType srcLabel, StringType tgtLabel,
                            EdgeRule edgeRule, DataBag outputBag) throws IOException {

        StringType currentSrcVertexName = new StringType(srcVertexName);
        StringType currentTgtVertexName = new StringType(tgtVertexName);

        addNewEdge(inputTupleInProgress, currentSrcVertexName, srcLabel,
                currentTgtVertexName, tgtLabel, edgeRule, outputBag);

        // Add both ends of the edge to the list of serialized property graph elements

        Vertex<StringType> srcVertex = createVertexWithDirection(currentSrcVertexName, srcLabel, LEFT);
        Vertex<StringType> tgtVertex = createVertexWithDirection(currentTgtVertexName, tgtLabel, RIGHT);
        addVertexToPropElementBag(outputBag, srcVertex);
        addVertexToPropElementBag(outputBag, tgtVertex);

        if (edgeRule.isBiDirectional()) {

            addNewEdge(inputTupleInProgress, currentTgtVertexName, tgtLabel,
                    currentSrcVertexName, srcLabel, edgeRule, outputBag);
        }
    }   // End of createEdge

    /**
     * Creates the edge object, adds the properties to it and adds the edge object to the output bag
     *
     * @param inputTupleInProgress
     * @param srcVertexName
     * @param srcLabel
     * @param tgtVertexName
     * @param tgtLabel
     * @param edgeRule
     * @param outputBag
     * @throws IOException
     */
    private void addNewEdge(InputTupleInProgress inputTupleInProgress, StringType srcVertexName,
                            StringType srcLabel, StringType tgtVertexName, StringType tgtLabel,
                            EdgeRule edgeRule, DataBag outputBag) throws IOException {

        Edge<StringType> edge = new Edge<StringType>(srcVertexName, srcLabel, tgtVertexName, tgtLabel,
                new StringType(edgeRule.getLabel(inputTupleInProgress)));

        for (String edgeAttribute : edgeRule.getPropertyFieldNames()) {
            Object propertyValue = inputTupleInProgress.getTupleData(edgeAttribute);

            if (propertyValue != null) {
                edge.setProperty(edgeAttribute, pigTypesToSerializedJavaTypes(propertyValue,
                        inputTupleInProgress.getType(edgeAttribute)));
            }
        }

        addEdgeToPropElementBag(outputBag, edge);
        incrementCounter(Counters.NUM_EDGES, 1L);

        if (isDangling(srcVertexName.get(), tgtVertexName.get()) && this.retainDanglingEdges) {
            incrementCounter(Counters.NUM_DANGLING_EDGES, 1L);
        }
    }


    /**
     * Increment counters for Hadoop Job in this UDF
     *
     * @param key   Enumerate counter name
     * @param value Increment value
     */
    public void incrementCounter(Enum key, Long value) {

        PigStatusReporter reporter = PigStatusReporter.getInstance();
        if (reporter != null) {
            Counter reporterCounter = reporter.getCounter(key);
            if (reporterCounter != null) {
                reporterCounter.increment(value);
            }
        }
    }

    /**
     * Parse a comma separated string and convert to a list of vertex names
     *
     * @param vertexCellString comma separated list. E.g. a,b,,d
     * @return List of "a, b, null, d" or "a, b, d" depending on retainDanglingEdges
     */
    private List<String> parseCommaSeparatedVertexList(String vertexCellString) {
        List<String> list = new ArrayList<String>();

        if (vertexCellString == null) {
            if (retainDanglingEdges) {
                list.add(NULL_VERTEX_NAME);
            }
        } else {
            for (String vertexName : expandString(vertexCellString)) {
                if (vertexName.isEmpty() && retainDanglingEdges) {
                    list.add(NULL_VERTEX_NAME);
                } else {
                    list.add(vertexName);
                }
            }
        }
        return list;
    }


    /**
     * Determines whether an edge is dangling or not
     *
     * @param srcVertexName
     * @param tgtVertexName
     * @return
     */
    private boolean isDangling(String srcVertexName, String tgtVertexName) {
        return srcVertexName.equals(NULL_VERTEX_NAME) || tgtVertexName.equals(NULL_VERTEX_NAME);
    }

    /**
     * Creates a new vertex data type with direction property "L" or "R"
     *
     * @param vertexName
     * @param label
     * @param direction
     * @return
     */
    private Vertex<StringType> createVertexWithDirection(StringType vertexName, StringType label, String direction) {
        Vertex<StringType> vertex = new Vertex<StringType>(vertexName, label);

        // If "-p" flag is enabled, then add the direction property to vertices
        // The direction property retains the original relationship direction as a vertex property
        // E.g., a data set with schema user, movie and ratings must create graph elements as
        // user1 "L" movie1 "R" rating1
        // movie1 "R" user1 "L" rating1, and not as
        // user1 "L" movie1 "R" rating1
        // user1 "R" movie1 "L" rating1
        // movie1 "R" user1 "L" rating1
        // movie1 "R" user1 "L" rating1
        // This property is not added in the reverse edge when bidirectional edges are enabled

        if (addSideToVertices) {
            vertex.setProperty(VERTEX_PROPERTY_SIDE, pigTypesToSerializedJavaTypes(direction, DataType.CHARARRAY));
        }
        return vertex;
    }


    /**
     * Provide return type information back to the Pig level.
     *
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

}
