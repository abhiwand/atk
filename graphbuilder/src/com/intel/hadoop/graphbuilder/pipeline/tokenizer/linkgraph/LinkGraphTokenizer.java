/* Copyright (C) 2012 Intel Corporation.
 *     All rights reserved.
 *           
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder 
 */
package com.intel.hadoop.graphbuilder.pipeline.tokenizer.linkgraph;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;

/**
 * A {@code GraphTokenizer} that converts a Wiki page (presented as
 * a string) into a set of vertices and edges by the following rule:
 * <ul>
 *     <li>There is a vertex for each page.</li>
 *     <li>There is an edge from page1 to page2 if page1 links to page2; the edge is labeled "linksTo".</li>
 * </ul>

 *
 * @see com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphTokenizer
 * @see LinkGraphBuildingRule
 * @see com.intel.hadoop.graphbuilder.pipeline.output.textgraph.TextGraphMR
 * @see com.intel.hadoop.graphbuilder.pipeline.input.text.TextParsingMapper
 */

public class LinkGraphTokenizer implements GraphTokenizer<String, StringType> {

    private static final Logger LOG = Logger.getLogger(LinkGraphTokenizer.class);

    public static final String LINKSTO = "linksTo";

    private String                        title;
    private List<String>                  links;
    private ArrayList<Vertex<StringType>> vertexList;
    private ArrayList<Edge<StringType>>   edgeList;



    private DocumentBuilderFactory factory;
    private DocumentBuilder        builder;
    private XPath                  xpath;

    /**
     * Allocates and initializes the parser and graph elements store.
     *
     */
    public LinkGraphTokenizer()  {

        factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);

        try {
            builder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.INTERNAL_PARSER_ERROR,
                    "Cannot configure XML parser for Link Graph tokenization", LOG, e);
        }

        XPathFactory xPathFactory = XPathFactory.newInstance();
        xpath = xPathFactory.newXPath();

        vertexList = new ArrayList<Vertex<StringType>>();
        edgeList   = new ArrayList<Edge<StringType>>();
        links      = new ArrayList<String>();
    }

    /**
     * Configures the tokenizer from the MR  configuration.
     * @param configuration   The MR configuration.
     */
    @Override
    public void configure(Configuration configuration) {
    }

    /**
     * Generates the property graph elements from parsing of a wiki page.
     * @param string  Wikipage presented as a string.
     * @param context  The Hadoop supplied mapper context.
     */
    public void parse(String string, Mapper.Context context) {

        try {

            Document doc = builder.parse(new InputSource(new StringReader(string)));

            title = xpath.evaluate("//page/title/text()", doc);
            title = title.replaceAll("\\s", "_");

            String text = xpath.evaluate("//page/revision/text/text()", doc);
            parseLinks(text);

        } catch (SAXException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.INTERNAL_PARSER_ERROR,
                    "Could not parse document", LOG, e);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "IO exception while parsing document", LOG, e);
        } catch (XPathExpressionException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.INTERNAL_PARSER_ERROR,
                    "Could not parse document", LOG, e);
        }
    }

    /**
     * Gets the vertex list for the wikipage.
     * @return   The iterator over vertex list.
     */
    public Iterator<Vertex<StringType>> getVertices() {

        vertexList.clear();
        vertexList.add(new Vertex<StringType>(new StringType(title)));

        for (String link : links) {
            vertexList.add(new Vertex<StringType>(new StringType(link)));
        }

        return vertexList.iterator();
    }

    /**
     * Gets the edge list for the wikipage.
     *
     * @return The iterator over the edge list.
     */
    @Override
    public Iterator<Edge<StringType>> getEdges() {

        if (links.isEmpty()) {
            return EmptyIterator.INSTANCE;
        }

        edgeList.clear();
        Iterator<String> iterator = links.iterator();
        final StringType LINKSTO_STYPE  = new StringType(LINKSTO);

        while (iterator.hasNext()) {
            edgeList.add(new Edge<StringType>(new StringType(title),
                    new StringType(iterator.next()), LINKSTO_STYPE));
        }

        return edgeList.iterator();
    }

    /*
     * This function is taken and modified from wikixmlj WikiTextParser
     */

    private void parseLinks(String text) {

        links.clear();

        Pattern catPattern = Pattern.compile("\\[\\[(.*?)\\]\\]", Pattern.MULTILINE);
        Matcher matcher    = catPattern.matcher(text);

        while (matcher.find()) {

            String[] potentialLink = matcher.group(1).split("\\|");

            if (potentialLink != null && potentialLink.length > 0) {

                String link = potentialLink[0];

                if (!link.replaceAll("\\s", "").isEmpty() && !link.contains(":")) {
                    links.add(link.replaceAll("\\s", "_"));
                }
            }
        }
    }
}
