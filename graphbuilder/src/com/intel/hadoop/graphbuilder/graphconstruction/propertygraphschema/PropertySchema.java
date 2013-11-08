package com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema;

/**
 * Encapsulates the name and datatype of a property label.
 * The expected use of this information is declaring keys for loading the constructed graph into a graph database.
 */

public class PropertySchema {

    private String    name;

    private Class<?>  type;

    private PropertySchema() {
        this.name = null;
        this.type = null;
    }

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
    }

    public Class<?> getType() {
        return this.type;
    }
}
