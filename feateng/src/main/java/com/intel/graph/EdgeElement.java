//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////


package com.intel.graph;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Represent an edge object in the graph
 */
public class EdgeElement implements IGraphElement {

    Map<String, Object> attributes = new HashMap<String, Object>();
    long id = 0;

    long inVertexId = 0;
    long outVertexId = 0;

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

    @Override
    public void writeToXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeStartElement(GraphMLTokens.EDGE);
        writer.writeAttribute(GraphMLTokens.ID, String.valueOf(getId()));
        writer.writeAttribute(GraphMLTokens.SOURCE, String.valueOf(getOutVertexId()));
        writer.writeAttribute(GraphMLTokens.TARGET, String.valueOf(getInVertexId()));

        writer.writeAttribute(GraphMLTokens.LABEL, "label");
        Collection<String> keys = getAttributes().keySet();
        for (String attributeKey : keys) {
            writer.writeStartElement(GraphMLTokens.DATA);
            writer.writeAttribute(GraphMLTokens.KEY, attributeKey);
            Object attributeValue = getAttributes().get(attributeKey);
            if (null != attributeValue) {
                writer.writeCharacters(attributeValue.toString());
            }
            writer.writeEndElement();
        }
        writer.writeEndElement();
    }

    /**
     * get incoming vertex's id
     * @return incoming vertex's id
     */
    public long getInVertexId() {
        return inVertexId;
    }

    /**
     * set incoming vertex' id
     * @param inVertexId: incoming vertex's id
     */
    public void setInVertexId(long inVertexId) {
        this.inVertexId = inVertexId;
    }

    /**
     * return outgoing vertex's id
     * @return outgoing vertex's id
     */
    public long getOutVertexId() {
        return outVertexId;
    }

    /**
     * set outgoing vertex's id
     * @param outVertexId: outgoing vertex's id
     */
    public void setOutVertexId(long outVertexId) {
        this.outVertexId = outVertexId;
    }
}
