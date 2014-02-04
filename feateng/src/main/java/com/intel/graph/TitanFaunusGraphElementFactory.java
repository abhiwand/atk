package com.intel.graph;


import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TitanFaunusGraphElementFactory implements IGraphElementFactory {
    @Override
    public IGraphElement makeElement(String text) {
        Matcher m_id = Pattern.compile("([0-9]*)").matcher(text);
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

        IGraphElement element;
        if(text.contains("_gb_ID"))  {
            element = new VertexElement(id);
        }
        else {
            element = new EdgeElement(id);
        }

        element.setAttributes(attributes);
        return element;
    }
}
