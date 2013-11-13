package mapreduce;

import org.apache.hadoop.util.ProgramDriver;

/**
 * Offers choices for included MapReduce jobs.
 */
public class Driver {

    /**
     * Main entry point for jar file.
     *
     * @param args  The command line parameters.
     * @throws Throwable When the selection fails.
     */
    public static void main(String[] args) throws Throwable {
	ProgramDriver pgd = new ProgramDriver();
	pgd.addClass(ImportFromFile.NAME, ImportFromFile.class,
		     "Import from file");
	pgd.addClass(ParseJson.NAME, ParseJson.class,
		     "Parse JSON into columns");
	pgd.addClass(ImportJsonFromFile.NAME,ImportJsonFromFile.class,
		     "Read json from hdfs to hbase");
	pgd.addClass(ParseRdf.NAME, ParseRdf.class,
		     "Parse Rdf into columns");
	pgd.addClass(ParseCsv.NAME, ParseCsv.class,
		     "Parse Csv into columns");
	pgd.addClass(BulkLoadXml.NAME, BulkLoadXml.class,
		     "Bulk load multi line XML to hbase column");
	pgd.addClass(ParseXml.NAME, ParseXml.class,
		     "Parse XML into columns");
	pgd.addClass(NewParseXml.NAME, NewParseXml.class,
		     "Parse XML into columns");
	pgd.addClass(ParseJson2.NAME, ParseJson2.class,
		     "Parse JSON into columns");
    
	pgd.addClass(ImportCsvFromFile.NAME, ImportCsvFromFile.class,
		     "Parse Csv directly from hdfs to hbase");
	pgd.addClass(ImportXmlFromFile.NAME, ImportXmlFromFile.class,
		     "Parse Xml directly from hdfs to hbase");
	pgd.driver(args);
    }
}
