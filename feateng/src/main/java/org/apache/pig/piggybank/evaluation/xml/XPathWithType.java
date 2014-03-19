//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2013 Intel Corporation All Rights Reserved.
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
package org.apache.pig.piggybank.evaluation.xml;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathConstants;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.w3c.dom.NodeList;
import org.apache.commons.lang3.StringUtils;

/**
 * XPathWithType is a UDF which allows for text extraction from xml given a type
 */
public class XPathWithType extends EvalFunc<String> {

    /** Hold onto last xpath & xml in case the next call to xpath() is feeding the same xml document
     * The reason for this is because creating an xpath object is costly. */
    private javax.xml.xpath.XPath xpath = null;
    private String xml = null;
    private Document document;
    
    private static boolean cache = true;
    
    /**
     * input should contain: 1) xml 2) xpath 3) type 4) optional cache xml doc flag
     * 
     * Usage:
     * 1) XPath(xml, xpath)
     * 2) XPath(xml, xpath, "Double")
     * 3) XPath(xml, xpath, "Long", false) 
     * 
     * @param 1st element should to be the xml
     *        2nd element should be the xpath
     *        3rd optional should be the desired data type of extracted element
     *        4th optional boolean cache flag (default true)
     *        
     * This UDF will cache the last xml document. This is helpful when multiple consecutive xpath calls are made for the same xml document.
     * Caching can be turned off to ensure that the UDF's recreates the internal javax.xml.xpath.XPath for every call
     * 
     * @return chararrary result or null if no match
     */
    @Override
    public String exec(final Tuple input) throws IOException {

        if (input == null || input.size() <= 1) {
            warn("Error processing input, not enough parameters or null input" + input,
                    PigWarning.UDF_WARNING_1);
            return null;
        }


        if (input.size() > 3) {
            warn("Error processing input, too many parameters" + input,
                    PigWarning.UDF_WARNING_1);
            return null;
        }

        try {

            final String xml = (String) input.get(0);
            
	    String type = "String";
            if(input.size() > 2) {
                type = (String) input.get(2);
            }
	    if (input.size() > 3) {
		cache = (Boolean) input.get(3);
	    }
            
            if(!cache || xpath == null || !xml.equals(this.xml))
            {
                final InputSource source = new InputSource(new StringReader(xml));
                
                this.xml = xml; //track the xml for subsequent calls to this udf

                final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                final DocumentBuilder db = dbf.newDocumentBuilder();
                
                this.document = db.parse(source);

                final XPathFactory xpathFactory = XPathFactory.newInstance();

                this.xpath = xpathFactory.newXPath();
                
            }
            
            final String xpathString = (String) input.get(1);

            String value = null;
//	    if (!type.equals("List")) 
//		    value = xpath.evaluate(xpathString, document);
//	    else {
		    NodeList nodeList = (NodeList) xpath.compile(xpathString).evaluate(document, XPathConstants.NODESET);
		    final ArrayList<String> array_values = new ArrayList<String>();
		    for (int i = 0; i < nodeList.getLength(); i++) {
			    array_values.add(nodeList.item(i).getFirstChild().getNodeValue()); 
		    }
		    value = StringUtils.join(array_values, ",");
//		    value = array_values.toString();
//	    }

	    try {
		    if (type.equals("Integer")) {
			    Integer result = NumberUtils.createInteger(value);
			    return String.valueOf(result);
		    } else if (type.equals("Boolean")) {
			    Boolean result = BooleanUtils.toBooleanObject(value);
			    return String.valueOf(result);
		    } else if (type.equals("Double")) {
			    Double result = NumberUtils.createDouble(value);
			    return String.valueOf(result);
		    } else if (type.equals("Float")) {
			    Float result = NumberUtils.createFloat(value);
			    return String.valueOf(result);
		    } else if (type.equals("Long")) {
			    Long result = NumberUtils.createLong(value);
			    return String.valueOf(result);
		    } else if (type.equals("BigInteger")) {
			    BigInteger result = NumberUtils.createBigInteger(value);
			    return String.valueOf(result);
		    } else if (type.equals("BigDecimal")) {
			    BigDecimal result = NumberUtils.createBigDecimal(value);
			    return String.valueOf(result);
		    } else {
			    return value;
		    }
	    } catch (Exception e) {
		    warn("Failed to process input; error - " + e.getMessage(),
				    PigWarning.UDF_WARNING_1);
		    return null;
	    }


        } catch (Exception e) {
            warn("Error processing input " + input.getType(0), 
                    PigWarning.UDF_WARNING_1);
            
            return null;
        }
    }

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {

		final List<FuncSpec> funcList = new ArrayList<FuncSpec>();

		/*either two chararray arguments*/
		List<FieldSchema> fields = new ArrayList<FieldSchema>();
		fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));

		Schema twoArgInSchema = new Schema(fields);

		funcList.add(new FuncSpec(this.getClass().getName(), twoArgInSchema));

		/*or three chararray arguments*/
		fields = new ArrayList<FieldSchema>();
		fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));

		Schema threeArgInSchema = new Schema(fields);

		funcList.add(new FuncSpec(this.getClass().getName(), threeArgInSchema));

		/*or three chararray and a boolean argument*/
		fields = new ArrayList<FieldSchema>();
		fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		fields.add(new Schema.FieldSchema(null, DataType.BOOLEAN));

		Schema fourArgInSchema = new Schema(fields);

		funcList.add(new FuncSpec(this.getClass().getName(), fourArgInSchema));

		return funcList;
	}

}

