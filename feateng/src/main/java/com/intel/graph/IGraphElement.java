package com.intel.graph;


import java.util.Map;

public interface IGraphElement {

    long getId();
    GraphElementType getElementType();
    Map<String, Object> getAttributes();
    void setAttributes(Map<String, Object> attributes);
}
