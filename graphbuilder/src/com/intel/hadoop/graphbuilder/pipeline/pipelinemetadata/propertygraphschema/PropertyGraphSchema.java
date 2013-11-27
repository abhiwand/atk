package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema;

import java.util.ArrayList;

/**
 * The schema or "signature" of a property graph. It contains all the possible types of edges and vertices that it may
 * contain. (It might contain types for edges or vertices that are not witnessed by any element present in the
 * graph.)
 *
 * The expected use of this information is declaring keys for loading the constructed graph into a graph database.
 */
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
