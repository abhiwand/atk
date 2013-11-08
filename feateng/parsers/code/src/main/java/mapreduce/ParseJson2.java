package mapreduce;

// cc ImportJsonFromFile Example job that reads from a file and writes into a table.

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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.io.IOException;


public class ParseJson2 {
    private static final Log LOG = LogFactory.getLog(ImportJsonFromFile.class);
    public enum Counters { ROWS, COLS, ERROR, VALID }
    public static final String NAME = "ParseJson2";
  
  
  
    /**
     * Implements the <code>Mapper</code> that takes the lines from the input
     * and outputs <code>Put</code> instances.
     */
    static class ImportMapper
	extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {

	private JSONParser parser = new JSONParser();
	private byte[] columnFamily = null;


    
	@Override
	    protected void setup(Context context)
	    throws IOException, InterruptedException {
	    columnFamily = Bytes.toBytes(
					 context.getConfiguration().get("conf.columnfamily"));

	    //if(type=="list")
	    //	extractIndices[i]=Integer.parseInt(str.trim());
	    
	    
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
		
			JSONObject json = (JSONObject) parser.parse(line.toString());
			
			for (Object key : json.keySet()) {
		            Object val = json.get(key);
		            String valString ="";
		            if (val == null)
		            	val = "null";
		            else
		            	valString = val.toString();
		            put.add(columnFamily, Bytes.toBytes(key.toString()),
				    Bytes.toBytes(valString));
			}
	
			
		
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
     * @throws org.apache.commons.cli.ParseException When the parsing of the
     *   parameters fails.
     */
    private static CommandLine parseArgs(String[] args) throws ParseException {
	// create options
	Options options = new Options();
	Option o = new Option("t", "table", true,
			      "table to import into (must exist)");
	o.setRequired(true);
	options.addOption(o);
	o = new Option("c", "column", true,
		       "column to read data from (must exist)");
	o.setArgName("family:qualifier");
	o.setRequired(true);
	options.addOption(o);
	o = new Option("i", "input", true,
		       "the directory in DFS to read files from");
	o.setRequired(true);
	options.addOption(o);
	options.addOption("d", "debug", false, "switch on DEBUG log level");
	
	
	// check if we are missing parameters
	if (args.length == 0) {
	    HelpFormatter formatter = new HelpFormatter();
	    formatter.printHelp(NAME + " ", options, true);
	    System.exit(-1);
	}
	CommandLineParser parser = new PosixParser();
	CommandLine cmd = parser.parse(options, args);
	// check debug flag first
	if (cmd.hasOption("d")) {
	    Logger log = Logger.getLogger("mapreduce");
	    log.setLevel(Level.DEBUG);
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
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	CommandLine cmd = parseArgs(otherArgs);
	// check debug flag and other options
	if (cmd.hasOption("d")) conf.set("conf.debug", "true");
	

	
	// get details
	String table = cmd.getOptionValue("t");
	String input = cmd.getOptionValue("i");
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
	// create job and set classes etc.
	Job job = new Job(conf, "Import from file " + input + " into table " + table);
	job.setJarByClass(ImportJsonFromFile.class);
	job.setMapperClass(ImportMapper.class);
	job.setOutputFormatClass(TableOutputFormat.class);
	job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
	job.setOutputKeyClass(ImmutableBytesWritable.class);
    
	job.setOutputValueClass(Writable.class);
	job.setNumReduceTasks(0);
	FileInputFormat.addInputPath(job, new Path(input));
	// run the job
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}