package com.intel.graph;

import java.util.HashMap;
import java.util.Map;

public class EdgeElement implements IGraphElement {

    Map<String, Object> attributes = new HashMap<String, Object>();
    long id = 0;

    public EdgeElement(long id) {
        this.id = id;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public GraphElementType getElementType() {
        return GraphElementType.Edge;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
