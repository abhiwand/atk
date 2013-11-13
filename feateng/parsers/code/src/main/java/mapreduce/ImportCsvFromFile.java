package mapreduce;

// cc ImportJsonFromFile Example job that reads from a file and writes into a table.
import mapreduce.ImportFromFile.Counters;
import mapreduce.ImportJsonFromFile.ImportMapper;
import mapreduce.ParseCsv.ParseMapper;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
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
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.IOException;

// vv ImportJsonFromFile
public class ImportCsvFromFile {
  private static final Log LOG = LogFactory.getLog(ImportCsvFromFile.class);
  public enum Counters { ROWS, COLS, ERROR, VALID }
  public static final String NAME = "ImportCsvFromFile";
  
  
  
  /**
   * Implements the <code>Mapper</code> that takes the lines from the input
   * and outputs <code>Put</code> instances.
   */
  static class ImportMapper
  extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {

    
    private byte[] columnFamily = null;
    String[] headerInfo;
    String delimiter;
    
    @Override
    protected void setup(Context context)
    throws IOException, InterruptedException {
      columnFamily = Bytes.toBytes(
        context.getConfiguration().get("conf.columnfamily"));
      
      Configuration conf = context.getConfiguration();
      headerInfo = conf.get("headers").split(",");
      delimiter = conf.get("delimiter");
      
    }
   

    /**
     * Maps the input.
     *
     * @param offset The current offset into the input file.
     * @param line The current line of the file.
     * @param context The task context.
     * @throws java.io.IOException When mapping the input fails.
     */
    @Override
    public void map(LongWritable offset, Text line, Context context)
    throws IOException {
      
    	try {
    		
    		String lineString = line.toString();
            byte[] rowkey = DigestUtils.md5(lineString); 
            Put put = new Put(rowkey);
            String[] tokens = line.toString().split(delimiter,-1);
            if(tokens.length!= headerInfo.length)	
          	  throw new IllegalArgumentException("Header info mismatch");
            for(int i = 0 ; i< tokens.length; i++){
          	  put.add(columnFamily, Bytes.toBytes(headerInfo[i]),Bytes.toBytes(tokens[i]) );
            }
           
        
            context.write(new ImmutableBytesWritable(rowkey), put);
            context.getCounter(Counters.VALID).increment(1);
        //String link = (String) json.get("link");
        //byte[] md5Url = DigestUtils.md5(link);
        //Put put = new Put(md5Url);
        //put.add(Bytes.toBytes("data"), Bytes.toBytes("link"), Bytes.toBytes(link));
        //context.write(new ImmutableBytesWritable(md5Url), put);
      } catch (Exception e) {
    	  e.printStackTrace();
          LOG.info("error parsing line ");
          LOG.info("skipping line");
          
          context.getCounter(Counters.ERROR).increment(1);
          LOG.info("failed lines "+ context.getCounter(Counters.ERROR) );
      }
    }
  }

  /**
   * Parse the command line parameters.
   *
   * @param args The parameters to parse.
   * @return The parsed command line.
   * @throws org.apache.commons.cli.ParseException When the parsing of the
   *   parameters fails.
   */
  private static CommandLine parseArgs(String[] args) throws ParseException {
	    Options options = new Options();
	    Option o = new Option("i", "input", true,
	      "hdfs input(must exist)");
	    o.setArgName("the directory in DFS to read files from");
	    o.setRequired(true);
	    options.addOption(o);
	    o = new Option("t", "table_output", true,
	      "table to write to (must exist)");
	    o.setArgName("output-table-name");
	    o.setRequired(true);
	    options.addOption(o);
	    o = new Option("c", "column", true,
	    	      "column to read data from (must exist)");
	    o.setArgName("family:qualifier");
	    options.addOption(o);
	    
	    o = new Option("h", "header", true,
	    		"header (must exist)");
	    o.setArgName("header");
	    o.setRequired(true);
	    options.addOption(o);
	    
	    options.addOption("g", "delimiter", true, "default delim is ,");
	  
	    options.addOption("d", "debug", false, "switch on DEBUG log level");
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
	    String delimiter = null;
	    if (cmd.hasOption("g")){
	    	delimiter = cmd.getOptionValue("g");
	    }
	    else
	    	delimiter = ",";
	    conf.set("delimiter", delimiter);
	    LOG.info("delimiter " + delimiter);
	    conf.set("headers",cmd.getOptionValue("h"));
	    // get details
	    String input = cmd.getOptionValue("i");
	    String table = cmd.getOptionValue("t");
	    String column = cmd.getOptionValue("c");

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

	    // vv ParseRdf

	    Job job = new Job(conf, "Parse data in " + input + ", write to " + table +
	      "(map only)");
	    job.setJarByClass(ImportCsvFromFile.class);
	    job.setMapperClass(ImportMapper.class);
	    job.setOutputFormatClass(TableOutputFormat.class);
	    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    
	    job.setOutputValueClass(Writable.class);
	    job.setNumReduceTasks(0);
	    FileInputFormat.addInputPath(job, new Path(input));
	   

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
// ^^ ImportJsonFromFile