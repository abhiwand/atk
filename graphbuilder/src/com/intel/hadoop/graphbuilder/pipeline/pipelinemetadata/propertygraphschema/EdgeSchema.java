package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema;

import java.util.ArrayList;

/**
 * The type of an edge declaration used in graph construction. It encapsulates the label of the edge, as well as
 * the names and datatypes of the properties that can be associated with edges of this type.
 *
 * The expected use of this information is declaring keys for loading the constructed graph into a graph database.
 */
public class EdgeSchema {

    private ArrayList<PropertySchema> propertySchemata;

    private String label;

    public EdgeSchema(String label) {
        this.label       = label;
        propertySchemata = new ArrayList<PropertySchema>();
    }
    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }
    public ArrayList<PropertySchema> getPropertySchemata() {
        return propertySchemata;
    }
}
