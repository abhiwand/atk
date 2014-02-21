package com.intel.graph;


import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TitanFaunusGraphElementFactory implements IGraphElementFactory {
    @Override
    public IGraphElement makeElement(String text) {
        Matcher m_id = Pattern.compile("([0-9]+)").matcher(text);
        if(!m_id.find()) {
            throw new RuntimeException("Failed to get graph element identifier");
        }

        long id = Long.parseLong(m_id.group(1));

        Matcher m_body = Pattern.compile("(\\{.*\\})").matcher(text);
        if(!m_body.find()) {
            throw new RuntimeException("Failed to get graph element attributes");
        }

        String attributesString = m_body.group(1).replace("{","").replace("}","");
        String[] attributeSetString = attributesString.split(",");
        Map<String, Object> attributes = new HashMap<String, Object>();
        for(String attributePairString : attributeSetString) {
            attributePairString = attributePairString.trim();
            String[] pair = attributePairString.split("=");
            attributes.put(pair[0], pair[1]);
        }


        if(text.contains("_gb_ID"))  {
            VertexElement vertex = new VertexElement(id);
            vertex.setAttributes(attributes);
            return vertex;
        }
        else {
            EdgeElement edge = new EdgeElement(id);
            edge.setAttributes(attributes);
            Matcher m_linking = Pattern.compile("([0-9]+-edge->[0-9]+)").matcher(text);
            if(!m_linking.find()) {
                throw new RuntimeException("Failed to edge linking information");
            }

            String edgeLinking = m_linking.group(1);
            String[] verticesIds = edgeLinking.split("-edge->");
            edge.setOutVertexId(Long.parseLong(verticesIds[0]));
            edge.setInVertexId(Long.parseLong(verticesIds[1]));
            return edge;
        }
    }
}
