package com.intel.hadoop.graphbuilder.util;


import com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB;
import com.intel.hadoop.graphbuilder.sampleapplications.TableToTextGraph;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
//import static org.mockito.Matchers.any;
//import static org.mockito.Mockito.doCallRealMethod;
//import static org.mockito.Mockito.verify;
import static junit.framework.Assert.fail;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.*;
import static org.powermock.api.support.membermodification.MemberMatcher.method;

@RunWith(PowerMockRunner.class)
@PrepareForTest(RuntimeConfig.class)
public class CommandLineInterfaceTest {

    private final static String configFile = System.getProperty("user.dir") + "/resources/test/graphbuilder.xml";
    private final static String outputPath = System.getProperty("user.dir") + "/resources/test/output";
    private static String[] haddopGenericOptionsExample1 = {"-conf", configFile, "-Dtest=test"};
    private static String[] haddopGenericOptionsExample2 = {"-conf", configFile, "-D", "test=test"};
    private CommandLineInterface spiedCLI;
    private Options options;
    private Option optionOne =  OptionBuilder.withLongOpt("one").withDescription("sample option one").hasArgs()
            .withArgName("Edge-Column-Name").create("1");;
    private Option optionTwo = OptionBuilder.withLongOpt("two").withDescription("sample option two").hasArgs().isRequired()
            .withArgName("Edge-Column-Name").create("2");

    HashMap<String, String> hadoopOptions = new HashMap<String, String>();

    @Before
    public void setUp(){
        CommandLineInterface cli = new CommandLineInterface();
        spiedCLI = spy(cli);

        options = new Options();

        options.addOption(optionOne);
        options.addOption(optionTwo);



        hadoopOptions.put("-conf", configFile);
        hadoopOptions.put("-D", "test=0");

    }

    @After
    public void tearDown(){
        spiedCLI = null;
        options = null;
        hadoopOptions = null;
    }

    @Test
    public void test_TableToGraphDB_cli_options(){

        //the options for the demo app
        HashMap<String, String> cliArgs = new HashMap <String, String>();
        cliArgs.put("t", "kd_sample_data");
        cliArgs.put("v", "cf:name=cf:age,cf:dept");
        cliArgs.put("e", "cf:name,cf:dept,worksAt");
        cliArgs.put("a", "");
        cliArgs.put("F", "");
        cliArgs.put("d", "directed edge");
        cliArgs.put("h", "");

        testDemoApp(TableToGraphDB.class, cliArgs);
    }

    @Test
    public void test_TableToTextGraph_cli_options(){

        //the options for the demo app
        HashMap<String, String> cliArgs = new HashMap <String, String>();
        cliArgs.put("t", "kd_sample_data");
        cliArgs.put("v", "cf:name=cf:age,cf:dept");
        cliArgs.put("e", "cf:name,cf:dept,worksAt");
        cliArgs.put("F", "");
        cliArgs.put("d", "directed edge");
        cliArgs.put("o", outputPath);
        cliArgs.put("h", "");

        testDemoApp(TableToTextGraph.class, cliArgs);
    }

    private void testDemoApp(Class klass, HashMap<String, String> demoAppCliArgs){
        CommandLineInterface demoAppCliOptions = Whitebox.getInternalState(klass, "commandLineInterface");
        spiedCLI.setOptions(demoAppCliOptions.getOptions());

        //test 100 random random command lines
        for(int count = 100; count >0; count--){
            //give me a sample command line in random order. the only restriction is that the hadoop options have to be
            //first
            String[] commandLineArgs = getRandomizedCommandLine((HashMap<String,String>)hadoopOptions.clone(), (HashMap<String,String>)demoAppCliArgs.clone());

            //check the command line against our options
            spiedCLI.checkCli(commandLineArgs);

            //this test the remaining arguments after it was parsed by the hadoop generic options parser
            //testRemainingHadoopArgs(10, spiedCLI);

            Iterator<Option> optionIterator = demoAppCliOptions.getOptions().getOptions().iterator();
            int remaining = 0;
            while(optionIterator.hasNext()){
                Option next = optionIterator.next();
                if(next.hasArg()){
                    remaining += 2;
                    testParsedOptions(next, demoAppCliArgs.get(next.getOpt()), spiedCLI);
                }else{
                    remaining++;
                    assertTrue(spiedCLI.hasOption(next.getOpt()));
                }
            }

            testRemainingHadoopArgs(remaining, spiedCLI);
        }
    }

    @Test
    public void test_hadoop_generic_options_are_parsed(){
        spiedCLI.checkCli(haddopGenericOptionsExample1);

        testRemainingHadoopArgs(0, spiedCLI);

        assertEquals("verify parsed conf option", configFile, spiedCLI.getGenericOptionsParser().getCommandLine().getOptionValue("conf"));
        assertEquals("verify parsed single option", "test=test", spiedCLI.getGenericOptionsParser().getCommandLine().getOptionValue("D"));

    }

    @Test
    public void test_hadoop_generic_options_are_parsed_reverse_order(){
        spiedCLI.checkCli(haddopGenericOptionsExample2);

        testRemainingHadoopArgs(0, spiedCLI);

        assertEquals("verify parsed conf option", configFile, spiedCLI.getGenericOptionsParser().getCommandLine().getOptionValue("conf"));
        assertEquals("verify parsed single option", "test=test", spiedCLI.getGenericOptionsParser().getCommandLine().getOptionValue("D"));
    }

    @Test
    public void test_custom_option_parsing(){
        spiedCLI.setOptions(options);

        String[] customArgs = {"-one", "we parsed -one", "-two", "we parsed -two"};
        String[] cmdArgs = (String[]) ArrayUtils.addAll(haddopGenericOptionsExample1, customArgs);
        spiedCLI.checkCli(cmdArgs);

        testRemainingHadoopArgs(4, spiedCLI);
        testParsedOptions(optionOne, "we parsed -one", spiedCLI);
        testParsedOptions(optionTwo, "we parsed -two", spiedCLI);
    }

    @Test
    public void test_custom_option_parsing_revers_order(){
        spiedCLI.setOptions(options);

        String[] customArgs = {"-two", "we parsed -two", "-one", "we parsed -one"};
        String[] cmdArgs = (String[]) ArrayUtils.addAll(haddopGenericOptionsExample1, customArgs);
        spiedCLI.checkCli(cmdArgs);

        testRemainingHadoopArgs(4, spiedCLI);
        testParsedOptions(optionOne, "we parsed -one", spiedCLI);
        testParsedOptions(optionTwo, "we parsed -two", spiedCLI);
        System.out.print("");
    }

    /**
     * given two hashmaps return a random order string array with all the key value pairs from both maps
     *
     * @param hadoopArgs the list of hadoop options to randomize
     * @param gbArgs the list of graph builder specific options to randomize
     * @return a string array that mimics the main input args array
     */
    public String[] getRandomizedCommandLine(HashMap<String, String> hadoopArgs, HashMap<String, String> gbArgs){
        ArrayList<String> args = new ArrayList<String>();
        int maxCount = hadoopArgs.size()*2;

        //randomize the hadoop options first since they always have to be first
        for(int count = 0; count < maxCount;){
            String key = getRandomKey(hadoopArgs);
            args.add("-" + key);
            count++;
            args.add(hadoopArgs.get(key));
            count++;
            hadoopArgs.remove(key);
        }


        while(gbArgs.size() > 0 ){
            String key = getRandomKey(gbArgs);
            //args[count] = key;
            args.add("-" + key);
            if(!gbArgs.get(key).isEmpty()){
                //args[count] = gbArgs.get(key);
                args.add(gbArgs.get(key));
            }
            gbArgs.remove(key);
        }

        return args.toArray(new String[args.size()]);
    }

    /**
     * give us a random key-value
     * @param hash the hash map to derive our random key-value pair from
     * @return the random selected key
     */
    private String getRandomKey(HashMap<String, String> hash){
        Random rand = new Random();

        String[] toArray = hash.keySet().toArray(new String[hash.size()]);

        return toArray[rand.nextInt(hash.size())];
    }

    /**
     * verify that the option is parsed and matches our expected value after it's parsed by the command line interface
     * @param option the option to verify against
     * @param expected the expected parsed value
     * @param cli instance of the CommandLineInterface class to assert against
     */
    private void testParsedOptions(Option option, String expected, CommandLineInterface cli){
        if( spiedCLI.hasOption(option.getOpt()) && spiedCLI.hasOption(option.getLongOpt())){
            assertTrue(String.format("check the parsed value against the short option: %s", option.getOpt()),
                    cli.getOptionValue(option.getOpt()) == expected);
            assertTrue(String.format("check the parsed value against the long option: %s", option.getOpt()),
                    cli.getOptionValue(option.getLongOpt()) == expected);
        } else {
            fail();
        }
    }

    /**
     * test the amount of remaining arguments we have after the hadoop parser is done.
     * @param remaining what is the expected amount of remaining arguments
     * @param cli the CommandLineInterface instance to grab the hadoopGenericParser
     */
    private void testRemainingHadoopArgs(int remaining, CommandLineInterface cli){
        assertEquals(String.format("look for the remaining args in this case we should have: %d ", remaining), remaining,
                cli.getGenericOptionsParser().getRemainingArgs().length);
    }
}
