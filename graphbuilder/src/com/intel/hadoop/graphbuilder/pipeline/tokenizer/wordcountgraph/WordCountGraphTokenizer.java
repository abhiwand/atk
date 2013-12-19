/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pipeline.tokenizer.wordcountgraph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Scanner;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.types.IntType;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;

import com.intel.hadoop.graphbuilder.types.StringType;

/**
 * The {@code GraphTokenizer} class that converts a Wiki page (presented as a set of vertices and edges
 * by the following rules:
 * <ul>
 *     <li>There is a vertex for each wiki page.</li>
 *     <li>There is a vertex for each word.</li>
 *     <li>There is a "contains" edge between every page and every word that it contains.</li>
 *     <li>The "contains" edge between a page and a word contains the frequency of the word in the page in the "wordCount"
 *     property.</li>
 * </ul>
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphTokenizer
 * @see WordCountGraphBuildingRule
 * @see com.intel.hadoop.graphbuilder.pipeline.output.textgraph.TextGraphMR
 * @see com.intel.hadoop.graphbuilder.pipeline.input.text.TextParsingMapper
 */

public class WordCountGraphTokenizer implements GraphTokenizer<String, StringType> {

    private static final Logger LOG = Logger.getLogger(WordCountGraphTokenizer.class);

    private String                   pageTitle;
    private HashMap<String, Integer> wordCountMap;
    private FileSystem               fileSystem;
    private HashSet<String>          dictionary;
    private HashSet<String>          stopWordsList;

    private PropertyGraphSchema      graphSchema;

    public static final String CONTAINS  = "contains";
    public static final String WORDCOUNT = "wordCount";

    // The tags used to mark each vertex name as either a "document" or a "word".
    // Documents are prefixed by 0, words are prefixed by 1...
    // todo:  investigate eliminating this legacy functionality,
    //        ie. dropping the DOCUMENT_TAG and WORD_TAG and prefixes.

    private final char DOCUMENT_TAG = '0';
    private final char WORD_TAG     = '1';


    /**
     * Configures the tokenizer from files.
     *
     * In particular, loads the dictionary and stopwords files if they are provided.
     */
    @Override
    public void configure(Configuration configuration) {

        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "GRAPHBUILDER_ERROR: Could not access file system from configuration.", LOG, e);
        }

        String dictPath = configuration.get("Dictionary");

        if (dictPath != null && !dictPath.isEmpty()) {
            try {
                loadDictionary(dictPath);
            } catch (IOException e) {
                GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNABLE_TO_LOAD_INPUT_FILE,
                        "GRAPHBUILDER_ERROR: Could not load dictionary file, path=" + dictPath, LOG, e);
            }
        }

        String stopWordsPath = configuration.get("StopWords");

        if (stopWordsPath != null && !stopWordsPath.isEmpty()) {
            try {
                loadStopWords(stopWordsPath);
            } catch (IOException e) {
                GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNABLE_TO_LOAD_INPUT_FILE,
                        "GRAPHBUILDER_ERROR: Could not load stopwords file, path=" + stopWordsPath, LOG, e);
            }
        }
    }

    /**
     * Generates vertices and edges from the wikipage.
     * <p>
     * <ul>
     *     <li>There is a "contains" edge between every page and every word that it contains.</li>
     *     <li>The "contains" edge between a page and a word contains the frequency of the word.</li>
     * </ul>
     * </p>
     * @param {@code inputString}  The wikipage as a string.
     * @param {@code context}      The Hadoop provided mapper context.
     */
    public void parse(String inputString, Mapper.Context context) {

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);

        wordCountMap = new HashMap<String, Integer>();

        try {
            DocumentBuilder builder;
            builder = factory.newDocumentBuilder();

            Document doc = builder.parse(new InputSource(new StringReader(inputString)));

            XPathFactory xPathFactory = XPathFactory.newInstance();
            XPath        xpath        = xPathFactory.newXPath();

            String input_title = xpath.evaluate("//page/title/text()", doc);

            input_title = input_title.replaceAll("\\s", "_");

            if (!(input_title.startsWith("Wikipedia:") ||
                    input_title.startsWith("Help:") ||
                    input_title.startsWith("Category:") ||
                    input_title.startsWith("Template:") ||
                    input_title.startsWith("MediaWiki:") ||
                    input_title.startsWith("Book:") ||
                    input_title.startsWith("Portal:") ||
                    input_title.startsWith("File:") ||
                    input_title.startsWith("List_of_")) ||
                    input_title.startsWith("List_of_Intel_")) {

                pageTitle   = input_title;
                String text = xpath.evaluate("//page/revision/text/text()", doc);

                if (!text.isEmpty()) {

                    Analyzer    analyzer = new StandardAnalyzer(Version.LUCENE_30);
                    TokenStream stream   = analyzer.tokenStream(null, new StringReader(text));

                    while (stream.incrementToken()) {

                        String token = stream.getAttribute(TermAttribute.class).term().toLowerCase();

                        if (dictionary != null && !dictionary.contains(token)) {
                            continue;
                        }

                        if (stopWordsList != null && stopWordsList.contains(token)) {
                            continue;
                        }

                        if (wordCountMap.containsKey(token)) {
                            wordCountMap.put(token, wordCountMap.get(token) + 1);
                        } else {
                            wordCountMap.put(token, 1);
                        }
                    }
                }
            } // end of if (!(input_title.startsWith("Wikipedia:") || ...   block
        } catch (ParserConfigurationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.INTERNAL_PARSER_ERROR,
                    "GRAPHBUILDER_ERROR: Parser configuration error.", LOG, e);
        } catch (SAXException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.INTERNAL_PARSER_ERROR,
                    "GRAPHBUILDER_ERROR: SAXException", LOG, e);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.INTERNAL_PARSER_ERROR,
                    "GRAPHBUILDER_ERROR: IO Exception", LOG, e);
        } catch (XPathExpressionException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.INTERNAL_PARSER_ERROR,
                    "GRAPHBUILDER_ERROR: XPathExpressionException", LOG, e);
        }
    }

    /**
     * Gets the list of vertices generated by the tokenizer for this wikipage.
     * @return  A list of vertices.
     */
    @Override
    public Iterator<Vertex<StringType>> getVertices() {

        if (wordCountMap.isEmpty()) {
            return EmptyIterator.INSTANCE;
        }

        ArrayList<Vertex<StringType>> vertexList = new ArrayList<Vertex<StringType>>(wordCountMap.size() + 1);

        vertexList.add(new Vertex<StringType>(new StringType(DOCUMENT_TAG + pageTitle)));

        Iterator<String> keyIterator = wordCountMap.keySet().iterator();

        while (keyIterator.hasNext()) {
            vertexList.add(new Vertex<StringType>(new StringType(WORD_TAG + keyIterator.next())));
        }

        return vertexList.iterator();
    }

    /**
     * Gets the list of edges generated by the tokenizer for this wikipage.
     * @return  A list of generated edges.
     */
    @Override
    public Iterator<Edge<StringType>> getEdges() {

        if (wordCountMap.isEmpty()) {
            return EmptyIterator.INSTANCE;
        }

        ArrayList<Edge<StringType>>      edgeList      = new ArrayList<Edge<StringType>>(wordCountMap.size());
        Iterator<Entry<String, Integer>> entryIterator = wordCountMap.entrySet().iterator();
        final StringType                 CONTAINS_STYPE      = new StringType(CONTAINS);

        while (entryIterator.hasNext()) {

            Entry<String, Integer> entry = entryIterator.next();
            Edge                   edge  = new Edge<StringType>(new StringType(DOCUMENT_TAG + pageTitle),
                                                                new StringType(WORD_TAG + entry.getKey()),
                                                                CONTAINS_STYPE);

            edge.setProperty("wordCount", new IntType(entry.getValue()));

            edgeList.add(edge);
        }

        return edgeList.iterator();
    }

    private void loadDictionary(String dictionaryPath) throws IOException {

        FileStatus[] stats = fileSystem.listStatus(new Path(dictionaryPath));
        dictionary         = new HashSet<String>();

        for (FileStatus stat : stats) {

            LOG.debug(("Load dictionary: " + stat.getPath().getName()));
            Scanner scanner = null;
            try {
                scanner = new Scanner(new BufferedReader(new InputStreamReader(fileSystem.open(stat.getPath()))));

                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    dictionary.add(line);
                }
            }  catch (IOException e) {
                throw e;
            } finally {
                if (scanner != null) {
                    scanner.close();
                }
            }
        }
    }

    private void loadStopWords(String stopWordsPath) throws IOException {

        FileStatus[] stats = fileSystem.listStatus(new Path(stopWordsPath));
        stopWordsList      = new HashSet<String>();

        for (FileStatus stat : stats) {
            LOG.debug(("Load stop words: " + stat.getPath().getName()));

            Scanner scanner = null;
            try {
                scanner = new Scanner(new BufferedReader(new InputStreamReader(fileSystem.open(stat.getPath()))));

                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    stopWordsList.add(line);
                }
            } catch (IOException e) {
                throw e;
            } finally {
                if (scanner != null) {
                    scanner.close();
                }
            }
        }
    }
}
