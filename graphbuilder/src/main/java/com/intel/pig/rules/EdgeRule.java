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

import java.io.IOException;
import java.util.ArrayList;
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
        DYNAMIC, STATIC
    }

    public static final String PREFIX_FOR_DYNAMIC_EDGE_LABEL  = "dynamic:";

    boolean               isBiDirectional;
    private String        srcFieldName;
    private String        dstFieldName;
    private String        labelRule;
    private List<String>  propertyFieldNames;
    private EdgeLabelType edgeLabelType;
    private String        edgeLabelFieldName;

    /**
     * Constructor must take source, destination, bi-directionality and edge label type as arguments.
     * <p>There is no public default constructor.</p>
     *
     * @param srcFieldName  column name from which to get source vertex
     * @param dstFieldName  column name from which to get destination vertex
     * @param biDirectional is this edge bidirectional or not?
     * @param labelRule     is the raw edge label as entered from command line
     */
    public EdgeRule(String srcFieldName, String dstFieldName, boolean biDirectional,  String labelRule,
                    List<String> edgePropertyFieldNames) {

        this.srcFieldName       = srcFieldName;
        this.dstFieldName       = dstFieldName;
        this.propertyFieldNames = new ArrayList<String>();
        this.isBiDirectional    = biDirectional;
        this.labelRule          = labelRule;
        this.edgeLabelType      = determineEdgeLabelType(labelRule);

        for (String edgePropertyFieldName : edgePropertyFieldNames) {
                addPropertyColumnName(edgePropertyFieldName);
        }

        this.edgeLabelFieldName = parseEdgeLabelRule(); // In case of DYNAMIC edge labels
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
    public EdgeLabelType determineEdgeLabelType(String labelRule) throws IllegalArgumentException {

        if (labelRule == null)
            throw new IllegalArgumentException("Invalid edge label: " + labelRule);

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

        }

        return labelFieldName;
    }

    /**
     * The command line format of dynamic edge label rules is "dynamic:fieldName"
     *
     * @return
     * @throws IllegalArgumentException
     */
    private String parseDynamicEdgeRule() throws IllegalArgumentException {
        String[] splitEdgeLabelRule = this.labelRule.split(PREFIX_FOR_DYNAMIC_EDGE_LABEL);

        if (splitEdgeLabelRule.length < 2)
            throw new IllegalArgumentException("Field name is required for dynamic edge labels : " + this.labelRule);

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

    public String getLabel(InputTupleInProgress inputTupleInProgress) throws IOException, NumberFormatException {
        String label;

        if (this.edgeLabelType == EdgeLabelType.STATIC) {
            label = this.getLabelRule();
        } else if (this.edgeLabelType == EdgeLabelType.DYNAMIC) {
            label = (String) inputTupleInProgress.getTupleData(this.edgeLabelFieldName);

            if (label == null)
                throw new IllegalArgumentException("Null edge label in tuple : " + inputTupleInProgress);

            if (label.isEmpty())
                throw new IllegalArgumentException("Empty edge label in tuple : " + inputTupleInProgress);
        } else {
            throw new IllegalArgumentException("Invalid edge label type: " + this.edgeLabelType);
        }

        return label;
    }

    public EdgeLabelType getEdgeLabelType() {
        return this.edgeLabelType;
    }

    public String getEdgeLabelFieldName() {
        return this.edgeLabelFieldName;
    }
}
