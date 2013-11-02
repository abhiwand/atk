package com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema;

import java.util.ArrayList;

public class EdgeSchema {

    private ArrayList<PropertySchema> propertySchemata;

    private String label;

    public EdgeSchema() {
        propertySchemata = new ArrayList<PropertySchema>();
    }
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
