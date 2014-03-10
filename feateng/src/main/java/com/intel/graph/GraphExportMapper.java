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
import java.util.HashSet;
import java.util.Set;

/**
 * Mapper takes in the graph query output and writes out the xml representation for each vertex and edge for reduce phase.
 */
public class GraphExportMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    IGraphElementFactory elementFactory = null;
    XMLOutputFactory xmlInputFactory = null;
    IFileOutputStreamGenerator outputStreamGenerator = new FileOutputStreamGenerator();

    Set<String> vertexFeatures = new HashSet<String>();
    Set<String> edgeFeatures = new HashSet<String>();

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

            if(GraphElementType.Vertex == element.getElementType())
                vertexFeatures.addAll(element.getAttributes().keySet());
            else
                edgeFeatures.addAll(element.getAttributes().keySet());

        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to generate xml node for the element", e);
        }  finally {
            f.close();
            GraphMLWriter.closeXMLWriter(writer);
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
            writeSchemaToXML(writer, vertexFeatures, edgeFeatures);

        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to export schema info from mapper", e);
        } finally {
            output.close();
            GraphMLWriter.closeXMLWriter(writer);
        }
    }


    /**
     * Taking the collected schema information and writing it to xml
     * @param writer: stream writer to xml output
     * @param vertexFeatures: feature names for vertex
     * @param edgeFeatures: feature names for edge
     * @throws XMLStreamException
     */
    public void writeSchemaToXML(XMLStreamWriter writer, Set<String> vertexFeatures, Set<String> edgeFeatures) throws XMLStreamException {
        writer.writeStartDocument();
        writer.writeStartElement(GraphExporter.SCHEMA);

        writeSchemaForElementType(writer, vertexFeatures, GraphElementType.Vertex.toString());
        writeSchemaForElementType(writer, edgeFeatures, GraphElementType.Edge.toString());

        writer.writeEndElement(); // schema
        writer.writeEndDocument();
        writer.flush();
    }

    private void writeSchemaForElementType(XMLStreamWriter writer, Set<String> vertexFeatures, String elementType) throws XMLStreamException {
        Set<String> features = vertexFeatures;
        for(String key : features) {
            writer.writeStartElement(GraphExporter.FEATURE);
            writer.writeAttribute(GraphMLTokens.ATTR_NAME, key);
            writer.writeAttribute(GraphMLTokens.ATTR_TYPE, "string");
            writer.writeAttribute(GraphMLTokens.FOR, elementType);
            writer.writeEndElement();
        }
    }
}

