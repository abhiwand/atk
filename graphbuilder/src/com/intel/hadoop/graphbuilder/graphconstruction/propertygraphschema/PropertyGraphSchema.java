package com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema;

import java.util.ArrayList;

public class PropertyGraphSchema {

    private ArrayList<VertexSchema> vertexSchemata;
    private ArrayList<EdgeSchema>   edgeSchemata;

    public PropertyGraphSchema() {
        vertexSchemata = new ArrayList<VertexSchema>();
        edgeSchemata   = new ArrayList<EdgeSchema>();
    }

    public void addVertexSchema(VertexSchema vertexSchema) {
        vertexSchemata.add(vertexSchema);
    }

    public ArrayList<VertexSchema> getVertexSchemata() {
        return vertexSchemata;
    }

    public void addEdgeSchema(EdgeSchema edgeSchema) {
        edgeSchemata.add(edgeSchema);
    }

    public ArrayList<EdgeSchema> getEdgeSchemata() {
        return edgeSchemata;
    }

}
