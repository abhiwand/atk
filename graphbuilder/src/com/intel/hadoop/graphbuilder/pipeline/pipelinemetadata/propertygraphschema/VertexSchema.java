package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema;

import java.util.ArrayList;

/**
 * The type of a vertex declaration used in graph construction. It encapsulates the names and datatypes of the
 * properties that can be associated with vertices of this type.
 *
 * The expected use of this information is declaring keys for loading the constructed graph into a graph database.
 */

public class VertexSchema {

    private ArrayList<PropertySchema> propertySchemata;

    public VertexSchema() {
        propertySchemata = new ArrayList<PropertySchema>();
    }

    public ArrayList<PropertySchema> getPropertySchemata() {
        return propertySchemata;
    }

}
