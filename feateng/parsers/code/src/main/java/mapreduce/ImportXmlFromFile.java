package mapreduce;

// cc Parse Xml MapReduce job that parses the raw data into separate columns (map phase only).


import org.apache.commons.cli.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ImportXmlFromFile {

  private static final Log LOG = LogFactory.getLog(ImportXmlFromFile.class);

  public static final String NAME = "ImportXmlFromFile";
  public enum Counters { ROWS, COLS, ERROR, VALID }

  /**
   * Implements the <code>Mapper</code> that reads the data and extracts the
   * required information.
   */
  static class ImportMapper
	extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> { 

    //private JSONParser parser = new JSONParser();
    private byte[] columnFamily = null;

    @Override
    protected void setup(Context context)
    throws IOException, InterruptedException {
      columnFamily = Bytes.toBytes(
        context.getConfiguration().get("conf.columnfamily"));
    }
    
    private void  getKeyValue(Element key,List children, ArrayList data) {
		//System.out.println(key);
		//System.out.println(children);
		Iterator itr = children.iterator();
		while(itr.hasNext()) {
			Element newKey = (Element) itr.next();
			//System.out.println(newKey.getName() + "==> " +newKey.getText());
			data.add(newKey.getName() + "=>" +newKey.getText());
			List newChildren = newKey.getChildren();
			
			getKeyValue(newKey,newChildren,data);
		}
		
	}

    /**
     * Maps the input.
     *
     * @param row The row key.
     * @param columns The columns of the row.
     * @param context The task context.
     * @throws java.io.IOException When mapping the input fails.
     */
    @Override
    public void map(LongWritable offset, Text line, Context context)
    throws IOException {
      context.getCounter(Counters.ROWS).increment(1);
     
      try {
    	  	String lineString = line.toString();
  			byte[] rowkey = DigestUtils.md5(lineString); // co ImportFromFile-4-RowKey The row key is the MD5 hash of the line to generate a random key.
  			Put put = new Put(rowkey);
  			SAXBuilder builder = new SAXBuilder();
  		    Reader in = new StringReader(lineString);
  		    
  	        Document doc = builder.build(in);
  	        Element root = doc.getRootElement();
  	        
  	        List children = root.getChildren();
  	        ArrayList<String> data = new ArrayList();
  	        getKeyValue(root,children,data);
  	        for (int i = 0; i < data.size(); i ++) {
  	        	String tokens[] = data.get(i).split("=>",-1);
  	        	put.add(columnFamily, Bytes.toBytes(tokens[0]),Bytes.toBytes(tokens[1]));
  	        }
  	        Integer count = data.size();
  	      //put.add(columnFamily, Bytes.toBytes("count"),Bytes.toBytes(count.toString()));
  	      context.write(new ImmutableBytesWritable(rowkey), put);
        context.getCounter(Counters.VALID).increment(1);
      } catch (Exception e) {
        e.printStackTrace();
       
        context.getCounter(Counters.ERROR).increment(1);
      }
    }

  }

  /**
   * Parse the command line parameters.
   *
   * @param args The parameters to parse.
   * @return The parsed command line.
   * @throws org.apache.commons.cli.ParseException When the parsing of the parameters fails.
   */
  private static CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();
    Option o = new Option("i", "input", true,
      "hdfs input(must exist)");
    o.setArgName("hdfs input");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("t", "table", true,
      "table to write to (must exist)");
    o.setArgName("output-table-name");
    o.setRequired(true);
    options.addOption(o);
    o = new Option("c", "column", true,
      "column to read data from (must exist)");
    o.setArgName("family:qualifier");
    options.addOption(o);
    options.addOption("d", "debug", false, "switch on DEBUG log level");
    
    o = new Option("r", "record", true,
		       "specify what ties a record together (must exist)");
	o.setArgName("record");
	o.setRequired(true);
	options.addOption(o);
    
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (Exception e) {
      System.err.println("ERROR: " + e.getMessage() + "\n");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(NAME + " ", options, true);
      System.exit(-1);
    }
    if (cmd.hasOption("d")) {
      Logger log = Logger.getLogger("mapreduce");
      log.setLevel(Level.DEBUG);
      System.out.println("DEBUG ON");
    }
    return cmd;
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs =
      new GenericOptionsParser(conf, args).getRemainingArgs();
    CommandLine cmd = parseArgs(otherArgs);
    // check debug flag and other options
    if (cmd.hasOption("d")) conf.set("conf.debug", "true");
    // get details
    String input = cmd.getOptionValue("i");
    String table = cmd.getOptionValue("t");
    String column = cmd.getOptionValue("c");
    String rawTag = cmd.getOptionValue("r").trim();
    StringBuilder startTag = new StringBuilder();
	startTag.append("<").append(rawTag).append(">");
	StringBuffer strbuf = new StringBuffer(startTag.toString());
	String endTag = strbuf.insert(1,"/").toString();
	LOG.info("START TAG "+ startTag);
	LOG.info("END TAG "+ endTag);
	conf.set("conf.column", column);
	conf.set("xmlinput.start", startTag.toString());
	conf.set("xmlinput.end", endTag);
	conf.set("io.serializations",
			 "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
	Scan scan = new Scan();
	if (column != null) {
	    byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
	    if (colkey.length > 1) {
	        scan.addColumn(colkey[0], colkey[1]);
	        conf.set("conf.columnfamily", Bytes.toStringBinary(colkey[0])); 
	        conf.set("conf.columnqualifier", Bytes.toStringBinary(colkey[1]));
	    } else {
	        scan.addFamily(colkey[0]);
	        conf.set("conf.columnfamily", Bytes.toStringBinary(colkey[0]));
	    }
	}
	// co ImportFromFile-8-JobDef Define the job with the required classes
	Job job = new Job(conf, "Import from file " + input + " into table " + table); 
	job.setJarByClass(ImportXmlFromFile.class);
	job.setMapperClass(ImportMapper.class);
	job.setInputFormatClass(XmlInputFormat.class);
	job.setOutputFormatClass(TableOutputFormat.class);
	job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
	job.setOutputKeyClass(ImmutableBytesWritable.class);
	job.setOutputValueClass(Writable.class);
	//This is a map only job, therefore tell the framework to bypass the reduce step.
	job.setNumReduceTasks(0); 
	FileInputFormat.addInputPath(job, new Path(input));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
