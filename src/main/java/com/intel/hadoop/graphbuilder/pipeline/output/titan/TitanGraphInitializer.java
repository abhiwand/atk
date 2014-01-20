/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.GraphDatabaseConnector;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import com.thinkaurelius.titan.core.KeyMaker;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanType;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Opens the Titan graph database, and make the Titan keys required by
 * the graph schema.
 */
public class TitanGraphInitializer {

    private static final Logger LOG = Logger.getLogger(TitanGraphInitializer.class);

    private Configuration conf;
    private PropertyGraphSchema graphSchema;
    private TitanGraph graph;
    private String keyCommandLine;

    /**
     * Used for making the Titan keys required by the graph schema
     * @param conf the Titan configuration
     * @param graphSchema
     * @param keyCommandLine the set of Titan Key definitions from the command line...
     */
    public TitanGraphInitializer(Configuration conf, PropertyGraphSchema graphSchema, String keyCommandLine) {
        this.conf = conf;
        this.graphSchema = graphSchema;
        this.keyCommandLine = keyCommandLine;
    }

    /*
     * Opens the Titan graph database, and make the Titan keys required by
     * the graph schema.
     */
    public void run() {
        initTitanGraphInstance(conf);

        HashMap<String, TitanKey> propertyNamesToTitanKeysMap =
                declareAndCollectKeys();

        // now we declare the edge labels
        // one of these days we'll probably want to fully expose all the
        // Titan knobs regarding manyToOne, oneToMany, etc

        for (EdgeSchema edgeSchema : graphSchema.getEdgeSchemata()) {
            ArrayList<TitanKey> titanKeys = new ArrayList<TitanKey>();

            for (PropertySchema propertySchema : edgeSchema
                    .getPropertySchemata()) {
                titanKeys.add(propertyNamesToTitanKeysMap.get(propertySchema
                        .getName()));
            }

            TitanKey[] titanKeyArray = titanKeys.toArray(new TitanKey[titanKeys.size()]);
            if (graph.getType(edgeSchema.getLabel()) == null) {
                graph.makeLabel(edgeSchema.getLabel()).signature(titanKeyArray).make();
            }
        }

        graph.commit();
    }

    /**
     * Creates the Titan graph for saving edges.
     */
    private void initTitanGraphInstance(Configuration configuration) {
        BaseConfiguration titanConfig = new BaseConfiguration();
        graph = GraphDatabaseConnector.open("titan", titanConfig, configuration);
    }

    /*
     * Gets the set of Titan Key definitions from the command line...
     */
    protected HashMap<String, TitanKey> declareAndCollectKeys() {

        HashMap<String, TitanKey> keyMap = new HashMap<String, TitanKey>();

        TitanKey gbIdKey = getOrCreateTitanKey(createGbId());
        keyMap.put(TitanConfig.GB_ID_FOR_TITAN, gbIdKey);

        List<GBTitanKey> declaredKeys = parseKeyCommandLine();
        for (GBTitanKey gbTitanKey : declaredKeys) {
            TitanKey titanKey = getOrCreateTitanKey(gbTitanKey);
            keyMap.put(titanKey.getName(), titanKey);
        }

        HashMap<String, Class<?>> propertyNameToTypeMap = graphSchema.getMapOfPropertyNamesToDataTypes();
        for (String property : propertyNameToTypeMap.keySet()) {

            if (!keyMap.containsKey(property)) {
                GBTitanKey gbTitanKey = new GBTitanKey(property);
                gbTitanKey.setDataType(propertyNameToTypeMap.get(property));
                TitanKey key = getOrCreateTitanKey(gbTitanKey);
                keyMap.put(property, key);
            }
        }
        return keyMap;
    }

    protected GBTitanKey createGbId() {
        // Because Titan requires combination of vertex names and vertex
        // labels into single strings for unique IDs the unique
        // GB_ID_FOR_TITAN property must be of StringType

        GBTitanKey gbIdDef = new GBTitanKey(TitanConfig.GB_ID_FOR_TITAN);
        gbIdDef.setDataType(String.class);
        gbIdDef.setIsVertexIndex(true);
        gbIdDef.setIsUnique(true);
        return gbIdDef;
    }

    /**
     * Get an existing key or create a new one
     * @param gbTitanKey a bean that describes the key
     * @return the actual key from Titan
     */
    protected TitanKey getOrCreateTitanKey(GBTitanKey gbTitanKey) {
        TitanKey titanKey = getTitanKey(gbTitanKey.getName());
        if (titanKey == null) {
            titanKey = createTitanKey(gbTitanKey);
        }
        return titanKey;
    }

    /**
     * Get an existing Titan key if it exists
     *
     * @param name of the key
     * @return the key or null if it does not yet exist
     */
    protected TitanKey getTitanKey(String name) {
        TitanKey titanKey = null;
        TitanType type = graph.getType(name);
        if (type != null) {
            titanKey = (TitanKey) type;
        }
        return titanKey;
    }

    /**
     * Create a new Titan Key
     *
     * @param gbTitanKey a bean that describes the key
     * @return the newly created key from Titan
     */
    protected TitanKey createTitanKey(GBTitanKey gbTitanKey) {

        KeyMaker keyMaker = graph.makeKey(gbTitanKey.getName());
        keyMaker.dataType(gbTitanKey.getDataType());

        if (gbTitanKey.isEdgeIndex()) {
            keyMaker.indexed(Edge.class);
        }
        if (gbTitanKey.isVertexIndex()) {
            keyMaker.indexed(Vertex.class);
        }
        if (gbTitanKey.isUnique()) {
            keyMaker.unique();
        }

        return keyMaker.make();
    }

    /*
     * This private method does the parsing of the command line -keys option
     * into a list of GBTitanKey objects.
     *
     * The -keys option takes a comma separated list of key rules.
     *
     * A key rule is:  <property name>;<option_1>; ... <option_n>
     * where the options are datatype specifiers, flags to use the key for
     * indexing edges and vertices, or a uniqueness bit,
     * per the definitions in TitanCommandLineOptions.
     *
     * Example:
     *    -keys  cf:userId;String;U;V,cf:eventId;E;Long
     *
     *    Generates a key for property cf:UserId that is a unique vertex
     *    index taking string values, and a key for property cf:eventId that
     *    is an edge index taking Long values.
     */
    protected List<GBTitanKey> parseKeyCommandLine() {

        ArrayList<GBTitanKey> gbKeyList = new ArrayList<GBTitanKey>();

        if (keyCommandLine.length() > 0) {

            String[] keyRules = keyCommandLine.split("\\,");

            for (String keyRule : keyRules) {
                String[] ruleProperties = keyRule.split(";");

                if (ruleProperties.length > 0) {
                    String propertyName = ruleProperties[0];

                    GBTitanKey gbTitanKey = new GBTitanKey(propertyName);

                    for (int i = 1; i < ruleProperties.length; i++) {
                        String ruleModifier = ruleProperties[i];

                        if (ruleModifier.equals(TitanCommandLineOptions
                                .STRING_DATATYPE)) {
                            gbTitanKey.setDataType(String.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.INT_DATATYPE)) {
                            gbTitanKey.setDataType(Integer.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.LONG_DATATYPE)) {
                            gbTitanKey.setDataType(Long.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.DOUBLE_DATATYPE)) {
                            gbTitanKey.setDataType(Double.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.FLOAT_DATATYPE)) {
                            gbTitanKey.setDataType(Float.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.VERTEX_INDEXING)) {
                            gbTitanKey.setIsVertexIndex(true);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.EDGE_INDEXING)) {
                            gbTitanKey.setIsEdgeIndex(true);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.UNIQUE)) {
                            gbTitanKey.setIsUnique(true);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.NOT_UNIQUE)) {
                            gbTitanKey.setIsUnique(false);
                        } else {
                            GraphBuilderExit.graphbuilderFatalExitNoException
                                    (StatusCode.BAD_COMMAND_LINE,
                                            "Error declaring keys.  " + ruleModifier
                                                    + " is not a valid option.\n" +
                                                    TitanCommandLineOptions
                                                            .KEY_DECLARATION_CLI_HELP,
                                            LOG);
                        }
                    }

                    // Titan requires that unique properties be vertex indexed
                    if (gbTitanKey.isUnique()) {
                        gbTitanKey.setIsVertexIndex(true);
                    }

                    gbKeyList.add(gbTitanKey);
                }
            }
        }

        return gbKeyList;
    }

    /** added for testing purposes */
    protected void setGraph(TitanGraph graph) {
        this.graph = graph;
    }

}
