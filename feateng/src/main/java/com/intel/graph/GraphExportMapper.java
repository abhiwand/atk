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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Mapper takes in the graph query output and writes out the xml representation for each vertex and edge for reduce phase.
 */
public class GraphExportMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    IGraphElementFactory elementFactory = null;
    XMLOutputFactory xmlInputFactory = null;
    IFileOutputStreamGenerator outputStreamGenerator = new FileOutputStreamGenerator();

    Map<String, GraphElementType> propertyElementTypeMapping = new HashMap<String, GraphElementType>();

    @Override
    protected void setup(Context context) {
        elementFactory = new TitanFaunusGraphElementFactory();
        xmlInputFactory = XMLOutputFactory.newInstance();
    }

    /**
     * Convert a line Faunus query output to a graph element object.
     * Then write the element's data as xml.
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ByteArrayOutputStream f = new ByteArrayOutputStream();
        XMLStreamWriter writer = null;
        try {
            writer = xmlInputFactory.createXMLStreamWriter(f);
            IGraphElement element = elementFactory.makeElement(value.toString());
            element.writeToXML(writer);
            context.write(key, new Text(f.toString()));
            // collects schema info
            collectSchemaInfo(element, propertyElementTypeMapping);
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to generate xml node for the element", e);
        }  finally {
            GraphMLWriter.closeXMLWriter(writer);
            f.close();
        }
    }

    /**
     * Iterating through the attribute name of the element. Record the attribute names and
     * associate them with the element's type in propertyElementTypeMapping
     * @param element: The graph element
     * @param propertyElementTypeMapping: The mapping between attribute name and the type of element that the attribute is from
     */
    public void collectSchemaInfo(IGraphElement element, Map<String, GraphElementType> propertyElementTypeMapping) {
        Set<String> keySet = element.getAttributes().keySet();
        for(String feature : keySet) {
            propertyElementTypeMapping.put(feature, element.getElementType());
        }
    }

    /**
     * Write the collected schema info to intermediate files.
     * The files will be read from reducer to write schema section in the
     * final graphml file.
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        TaskAttemptID id = context.getTaskAttemptID();
        Path path = new Path(new File(TextOutputFormat.getOutputPath(context).toString(), GraphExporter.METADATA_FILE_PREFIX + id.toString()).toString());

        OutputStream output = outputStreamGenerator.getOutputStream(context, path);
        final XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter writer = null;
        try {
            writer = outputFactory.createXMLStreamWriter(output, "UTF8");
            writeSchemaToXML(writer, propertyElementTypeMapping);

        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to export schema info from mapper", e);
        } finally {
            GraphMLWriter.closeXMLWriter(writer);
            output.close();
        }
    }


    /**
     * Taking the collected schema information and writing it to xml
     * @param writer: stream writer to xml output
     * @param propertyElementTypeMapping mapping between attribute name and element type
     * @throws XMLStreamException
     */
    public void writeSchemaToXML(XMLStreamWriter writer, Map<String, GraphElementType> propertyElementTypeMapping) throws XMLStreamException {
        writer.writeStartDocument();
        writer.writeStartElement(GraphExporter.SCHEMA);

        for(Map.Entry<String, GraphElementType> e : propertyElementTypeMapping.entrySet()) {
            writer.writeStartElement(GraphExporter.FEATURE);
            writer.writeAttribute(GraphMLTokens.ATTR_NAME, e.getKey());
            writer.writeAttribute(GraphMLTokens.ATTR_TYPE, "string");
            writer.writeAttribute(GraphMLTokens.FOR, e.getValue().toString());
            writer.writeEndElement();
        }

        writer.writeEndElement(); // schema
        writer.writeEndDocument();
        writer.flush();
        writer.close();
    }
}

