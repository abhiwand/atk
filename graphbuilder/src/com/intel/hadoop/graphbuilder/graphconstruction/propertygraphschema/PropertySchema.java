package com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema;


public class PropertySchema {

    private String    name;

    private Class<?>  type;

    public PropertySchema(String name, Class<?> type) {
        this.name = name;
        this.type = type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setType(Class<?> type) {
        this.type = type;
        // nls todo:  look into validating the type against what's legal for the target data sink
    }

    public Class<?> getType() {
        return this.type;
    }
}
