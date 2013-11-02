package com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema;

import java.util.ArrayList;

public class VertexSchema {

    private ArrayList<PropertySchema> propertySchemata;

    public VertexSchema() {
        propertySchemata = new ArrayList<PropertySchema>();
    }

    public ArrayList<PropertySchema> getPropertySchemata() {
        return propertySchemata;
    }

}
