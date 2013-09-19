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

import com.intel.hadoop.graphbuilder.types.IntType;
import com.intel.hadoop.graphbuilder.types.PropertyMapType;
import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.intel.hadoop.graphbuilder.graphElements.Edge;
import com.intel.hadoop.graphbuilder.graphElements.Vertex;
import com.intel.hadoop.graphbuilder.graphConstruction.GraphTokenizer;
import com.intel.hadoop.graphbuilder.types.StringType;

public class WordCountGraphTokenizer implements
    GraphTokenizer<StringType> {

  private static final Logger LOG = Logger
      .getLogger(WordCountGraphTokenizer.class);

  @Override
  public void configure(JobConf job) {
    try {
      fs = FileSystem.get(job);
    } catch (IOException e1) {
      e1.printStackTrace();
    }

    String dictPath = job.get("Dictionary");
    if (dictPath != null && dictPath != "") {
      try {
        loadDictionary(dictPath);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    String stopWordsPath = job.get("StopWords");
    if (stopWordsPath != null && stopWordsPath != "") {
      try {
        loadStopWords(stopWordsPath);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public Class vidClass() {
    return StringType.class;
  }


  public void parse(String s) {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder;
    counts = new HashMap<String, Integer>();
    try {
      builder = factory.newDocumentBuilder();
      Document doc = builder.parse(new InputSource(new StringReader(s)));
      XPathFactory xfactory = XPathFactory.newInstance();
      XPath xpath = xfactory.newXPath();
      String input_title = xpath.evaluate("//page/title/text()", doc);
      input_title = input_title.replaceAll("\\s", "_");
      // title = title.replaceAll("^[^a-zA-Z0-9]", "#");
      // title = title.replaceAll("[^a-zA-Z0-9.]", "_");
     if(!( input_title.startsWith("Wikipedia:") ||
           input_title.startsWith("Help:") ||
	  	     input_title.startsWith("Category:") ||
	         input_title.startsWith("Template:") ||
	         input_title.startsWith("MediaWiki:") ||
	         input_title.startsWith("Book:") ||
	         input_title.startsWith("Portal:") ||
	         input_title.startsWith("File:") ||
	         input_title.startsWith("List_of_") ) || 
	         input_title.startsWith("List_of_Intel_")){
	        title = input_title;
          id = xpath.evaluate("//page/id/text()", doc);
          String text = xpath.evaluate("//page/revision/text/text()", doc);
    
          if (!text.isEmpty()) {
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
            TokenStream stream = analyzer.tokenStream(null, new StringReader(text));
            while (stream.incrementToken()) {
              String token = stream.getAttribute(TermAttribute.class).term().toLowerCase();
    
              if (dictionary != null && !dictionary.contains(token))
                continue;
    
                   if (stopWordsList != null && stopWordsList.contains(token))
                        continue;
    
              if (counts.containsKey(token))
                counts.put(token, counts.get(token) + 1);
              else
                counts.put(token, 1);
            }
          }
        }
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
    if (counts.isEmpty())
      return EmptyIterator.INSTANCE;
	
      ArrayList<Vertex<StringType>> vlist = new ArrayList<Vertex<StringType>>(
        counts.size() + 1);
          vlist.add(new Vertex<StringType>(new StringType(doc+title)));
          Iterator<String> iter = counts.keySet().iterator();
          while (iter.hasNext()) {
            vlist.add(new Vertex<StringType>(new StringType(word+iter.next())));
          }
    return vlist.iterator();
  }

  @Override
  public Iterator<Edge<StringType>> getEdges() {
    if (counts.isEmpty())
      return EmptyIterator.INSTANCE;

    ArrayList<Edge<StringType>> elist = new ArrayList<Edge<StringType>>(
        counts.size());
    Iterator<Entry<String, Integer>> iter = counts.entrySet().iterator();

    final StringType CONTAINS = new StringType("contains");

    while (iter.hasNext()) {
      Entry<String, Integer> e = iter.next();
      Edge edge = new Edge<StringType>(new StringType(doc+title),
                                       new StringType(word+e.getKey()),
                                       CONTAINS);
      edge.setProperty("wordCount", new IntType(e.getValue()));
      elist.add(edge);
    }
    return elist.iterator();
  }

  private void loadDictionary(String path) throws IOException {
    FileStatus[] stats = fs.listStatus(new Path(path));
    dictionary = new HashSet<String>();
    for (FileStatus stat : stats) {
      LOG.debug(("Load dictionary: " + stat.getPath().getName()));
      Scanner sc = new Scanner(new BufferedReader(new InputStreamReader(
          fs.open(stat.getPath()))));
      while (sc.hasNextLine()) {
        String line = sc.nextLine();
        dictionary.add(line);
      }
    }
  }

  private void loadStopWords(String path) throws IOException {
    FileStatus[] stats = fs.listStatus(new Path(path));
    stopWordsList = new HashSet<String>();
    for (FileStatus stat : stats) {
      LOG.debug(("Load stop words: " + stat.getPath().getName()));
      Scanner sc = new Scanner(new BufferedReader(new InputStreamReader(
          fs.open(stat.getPath()))));
      while (sc.hasNextLine()) {
        String line = sc.nextLine();
        stopWordsList.add(line);
      }
    }
  }

  private String title;
  private String id;
  private char doc ='0';
  private char word ='1';
  private HashMap<String, Integer> counts;
  private FileSystem fs;
  private HashSet<String> dictionary;
  private HashSet<String> stopWordsList;
}
