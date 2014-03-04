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
package com.intel.pig.rules;

import com.intel.pig.udf.util.InputTupleInProgress;
import org.apache.commons.math3.util.Pair;
import org.jruby.RubyIconv;

import java.io.IOException;
import java.nio.channels.IllegalChannelGroupException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Encapsulation of the rules for creating edges.
 * <p/>
 * <p> Edge rules are specified from command line as
 *       <source_vertex_field_name>,<target_vertex_field_name>,<label>,[<edge_prop1_field_name>,...] </p>
 * <p> Edge rules consist of the following:
 * <ul>
 * <li> A field name from which to read the edge's source vertex</li>
 * <li> A field name from which to read the edge's destination vertex</li>
 * <li> A boolean flag denoting if the edge is bidirectional or directed</li>
 * <li> A list of field names from which to read the edge's properties</li>
 * </ul></p>
 * <p>Edge rules are indexed by their label, so we do not store the label in the rule. </p>
 *
 * <ul>
 *       The rule for edge labels <label> is composite which are mutually exclusive to each other:
 * <li> String constants:  Edge labels can be constant string as LIKES or KNOWS. Constant edge
 *                         labels must not contain comma, dot or colon special characters </li>
 * <li> Range:             A range of edge labels can also be passed to the CreatePropGraphElements
 *                         constructor. Declaring a range for the edge label avoids a whole data scan to
 *                         create types of the label values in the Titan graph. Only integers are allowed for range
 *                         declarations. Other data types will throw an OutOfRange exception. </li>
 * <li> Dynamic edge labels: Edge labels can be created from fields of the input Pig tuple. Prefix the field
 *                         name with 'dynamic:' to annotate that this edge label must be created from the
 *                         tuple data </li>
 * </ul></p>
 *
 * TODO - The default source and target vertex names and edge property names are fields in Pig tuple,
 * TODO   whereas the edge label is a constant string. Dynamic edge labels which are derived from Pig tuple is
 * TODO   prefixed by 'dynamic:'. This inconsistency needs to be updated with the default being read from
 * TODO   Pig tuple and constant strings annotated by 'static:'
 */
public class EdgeRule {

    public enum EdgeLabelType {
        DYNAMIC, RANGE, STATIC
    }

    public static final String PREFIX_FOR_DYNAMIC_EDGE_LABEL  = "dynamic:";
    public static final String PREFIX_FOR_RANGE_EDGE_LABEL    = "range:";
    public static final String RANGE_SEPARATOR                = "::";

    boolean               isBiDirectional;
    private String        srcFieldName;
    private String        dstFieldName;
    private String        labelRule;
    private List<String>  propertyFieldNames;
    private EdgeLabelType edgeLabelType;
    private String[]      edgeAttributes;
    private String        edgeLabelFieldName;

    private List<Pair<Integer, Integer>> buckets;

    /**
     * Constructor must take source, destination, bi-directionality and edge label type as arguments.
     * <p>There is no public default constructor.</p>
     *
     * @param srcFieldName  column name from which to get source vertex
     * @param dstFieldName  column name from which to get destination vertex
     * @param biDirectional is this edge bidirectional or not?
     * @param labelRule     is the raw edge label as entered from command line
     */
    public EdgeRule(String srcFieldName, String dstFieldName, boolean biDirectional,  String labelRule) {
        this.srcFieldName       = srcFieldName;
        this.dstFieldName       = dstFieldName;
        this.propertyFieldNames = new ArrayList<String>();
        this.isBiDirectional    = biDirectional;
        this.edgeLabelType      = determineEdgeLabelType(labelRule);
        this.labelRule          = labelRule;

        List<String> edgeAttributeList = getPropertyFieldNames();
        this.edgeAttributes = edgeAttributeList.toArray(new String[edgeAttributeList.size()]);

        if (this.edgeLabelType == EdgeLabelType.RANGE) {
            this.buckets = new ArrayList<Pair<Integer, Integer>>();
        }

        this.edgeLabelFieldName = parseEdgeLabelRule(); // In case of DYNAMIC and RANGE edge labels
    }

    public String getSrcFieldName() {
        return this.srcFieldName;
    }

    public String getDstFieldName() {
        return this.dstFieldName;
    }

    public String getLabelRule() {
        return this.labelRule;
    }

    public boolean isBiDirectional() {
        return this.isBiDirectional;
    }

    /** Determines the type of the edge label entered from the edge rule entered from command line
     * @param labelRule
     * @return
     */
    public EdgeLabelType determineEdgeLabelType(String labelRule) {
        if (labelRule.contains(EdgeRule.PREFIX_FOR_RANGE_EDGE_LABEL)) {
            if (labelRule.contains("[") && labelRule.contains("]"))
                return EdgeLabelType.RANGE;
            else
                throw new IllegalArgumentException("Invalid edge rule: " + labelRule);
        }

        if (labelRule.contains(EdgeRule.PREFIX_FOR_DYNAMIC_EDGE_LABEL)) {
            return EdgeLabelType.DYNAMIC;
        } else {
            return EdgeLabelType.STATIC;
        }
    }

    /**
     * Parse dynamic or range edge label rules
     *
     * @return field name from which the edge label will be picked up from
     */
    private String parseEdgeLabelRule() throws IllegalArgumentException {

        String labelFieldName = null;

        if (this.edgeLabelType == EdgeLabelType.DYNAMIC) {

            labelFieldName = parseDynamicEdgeRule();

        } else if (this.edgeLabelType == EdgeLabelType.RANGE) {

            labelFieldName = parseRangeEdgeRule();

        }

        return labelFieldName;
    }

    /**
     * The command line format of range dge label rules is "range:[lower1-upper1::mid::lower2-upper2]:fieldName"
     *
     * @return
     * @throws IllegalArgumentException
     */
    private String parseRangeEdgeRule() throws IllegalArgumentException {

        String[] splitEdgeLabelRule = labelRule.split(PREFIX_FOR_RANGE_EDGE_LABEL);

        throwEmptyEdgeLabelException(splitEdgeLabelRule[1]);

        String edgeLabelRangesWithSeparator = splitEdgeLabelRule[1].replaceAll("\\[", "");
        //edgeLabelRangesWithSeparator = edgeLabelRangesWithSeparator.replaceAll("\\]", "");
        String[] suffixedFieldName = edgeLabelRangesWithSeparator.split("\\]\\:");

        throwEmptyEdgeLabelException(suffixedFieldName[0]);

        String[] ranges = suffixedFieldName[0].split("\\:\\:");

        for(String range : ranges) {
            if (range.contains("-")) {
                String[] lowerAndUpper = range.split("\\-");
                buckets.add(new Pair<Integer, Integer>(Integer.parseInt(lowerAndUpper[0]),
                        Integer.parseInt(lowerAndUpper[1])));
            } else {
                Integer value = Integer.parseInt(range);
                buckets.add(new Pair<Integer, Integer>(value, value));
            }
        }

        String[] splitFieldName = splitEdgeLabelRule[1].split("\\]\\:");

        throwEmptyEdgeLabelException(splitFieldName[1]);

        return splitFieldName[1];
    }

    /**
     * The command line format of dynamic edge label rules is "dynamic:fieldName"
     *
     * @return
     * @throws IllegalArgumentException
     */
    private String parseDynamicEdgeRule() throws IllegalArgumentException {
        String[] splitEdgeLabelRule = this.labelRule.split(PREFIX_FOR_DYNAMIC_EDGE_LABEL);

        throwEmptyEdgeLabelException(splitEdgeLabelRule[1]);

        return splitEdgeLabelRule[1];
    }

    private void throwEmptyEdgeLabelException(String s) throws IllegalArgumentException {
        if (s == null) {
            throw new IllegalArgumentException("Invalid edge rule: " + this.labelRule);
        } else if (s.isEmpty()) {
            throw new IllegalArgumentException("Invalid edge rule: " + this.labelRule);
        }
    }

    public void addPropertyColumnName(String columnName) {
        propertyFieldNames.add(columnName);
    }

    public List<String> getPropertyFieldNames() {
        return propertyFieldNames;
    }

    public String[] getEdgeAttributes() {
        return this.edgeAttributes;
    }

    public String getLabel(InputTupleInProgress inputTupleInProgress) throws IOException, NumberFormatException {
        String label;

        if (this.edgeLabelType == EdgeLabelType.STATIC) {
            label = this.getLabelRule();
        } else if (this.edgeLabelType == EdgeLabelType.RANGE || this.edgeLabelType == EdgeLabelType.DYNAMIC) {
            label = (String) inputTupleInProgress.getTupleData(this.edgeLabelFieldName);
            Integer labelToInt = Integer.parseInt(label);
            if (!withinRange(labelToInt))
                throw new IllegalArgumentException("Invalid edge label: " + inputTupleInProgress);
        } else {
            throw new IllegalArgumentException("Invalid edge label type: " + this.edgeLabelType);
        }

        return label;
    }

    private boolean withinRange(int key) {
        boolean result = false;

        for (Iterator<Pair<Integer, Integer>> iterator = buckets.iterator(); iterator.hasNext(); ) {
            Pair<Integer, Integer> next = iterator.next();
            if (key >= next.getFirst() && key < next.getSecond())
                result = true;
        }
        return result;
    }
}
