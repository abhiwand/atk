package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema;

/**
 * Encapsulates the name and datatype of a property label.
 * <p>The expected use of this information is declaring keys for loading the constructed graph into a graph database.</p>
 */

public class PropertySchema {

    private String    name;

    private Class<?>  type;

    /**
     * Constructor.
     * @param name name of the property
     * @param type datatype of the property, a {@code Class<?>} object
     */
    public PropertySchema(String name, Class<?> type) {
        this.name = name;
        this.type = type;
    }

    /**
     * Set the name of the property.
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the name of the property.
     * @return
     */
    public String getName() {
        return this.name;
    }

    /**
     * Set the datatype of the property.
     * @param type  datatype of the property
     */
    public void setType(Class<?> type) {
        this.type = type;
    }

    /**
     * Get the datatype of the property
     * @return  datatype of the property
     */
    public Class<?> getType() {
        return this.type;
    }
}
