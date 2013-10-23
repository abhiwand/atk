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

package com.intel.hadoop.graphbuilder.demoapps.wikipedia.docwordgraph;

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

import com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.types.IntType;
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
 * WordCountGraphTokenizer is a GraphTokenizerFromString class that is  called by the mapper to convert a Wiki page (presented as
 * a string) into a set of vertices and edges by the following rule:
 * - there is a vertex for each page
 * - there is a vertex for each word
 * - there is an edge between a page-vertex and a word-vertex if the word appears in the page
 * -- the edge is labeled "contains
 * -- the edge has a property "wordCount" that denotes the number of appearances of the word in the page
 *
 * @see com.intel.hadoop.graphbuilder.graphconstruction.tokenizer.GraphTokenizer
 * @see CreateWordCountGraph
 * @see com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.textgraph.TextGraphMR
 * @see com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.TextParsingMapper
 */

public class WordCountGraphTokenizer implements GraphTokenizer<String, StringType> {

    private static final Logger LOG = Logger.getLogger(WordCountGraphTokenizer.class);

    private String                   pageTitle;
    private HashMap<String, Integer> wordCountMap;
    private FileSystem               fileSystem;
    private HashSet<String>          dictionary;
    private HashSet<String>          stopWordsList;

    // tags used to mark each vertex name as either a "document" or a "word"
    // documents are prefixed by 0, words are prefixed by 1...
    // nls todo:  investigate eliminating this legacy functionality

    private final char DOCUMENT_TAG = '0';
    private final char WORD_TAG     = '1';

    @Override
    public void configure(Configuration configuration) {

        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        String dictPath = configuration.get("Dictionary");

        if (dictPath != null && dictPath != "") {
            try {
                loadDictionary(dictPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String stopWordsPath = configuration.get("StopWords");

        if (stopWordsPath != null && stopWordsPath != "") {
            try {
                loadStopWords(stopWordsPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Class vidClass() {
        return StringType.class;
    }

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
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }
    }

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

    @Override
    public Iterator<Edge<StringType>> getEdges() {

        if (wordCountMap.isEmpty()) {
            return EmptyIterator.INSTANCE;
        }

        ArrayList<Edge<StringType>>      edgeList      = new ArrayList<Edge<StringType>>(wordCountMap.size());
        Iterator<Entry<String, Integer>> entryIterator = wordCountMap.entrySet().iterator();
        final StringType                 CONTAINS      = new StringType("contains");

        while (entryIterator.hasNext()) {

            Entry<String, Integer> entry = entryIterator.next();
            Edge                   edge  = new Edge<StringType>(new StringType(DOCUMENT_TAG + pageTitle),
                                                                new StringType(WORD_TAG + entry.getKey()),
                                                                CONTAINS);

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

            Scanner scanner = new Scanner(new BufferedReader(new InputStreamReader(fileSystem.open(stat.getPath()))));

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                dictionary.add(line);
            }
        }
    }

    private void loadStopWords(String stopWordsPath) throws IOException {

        FileStatus[] stats = fileSystem.listStatus(new Path(stopWordsPath));
        stopWordsList      = new HashSet<String>();

        for (FileStatus stat : stats) {
            LOG.debug(("Load stop words: " + stat.getPath().getName()));

            Scanner scanner = new Scanner(new BufferedReader(new InputStreamReader(fileSystem.open(stat.getPath()))));

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                stopWordsList.add(line);
            }
        }
    }
}
