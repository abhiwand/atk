package com.intel.hadoop.graphbuilder.util;


import com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB;
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

    private final static HashMap<String,String> configHadoopOptions = new HashMap<String,String>();
    static{
        //HashMap<String,String> temp = new HashMap<String,String>();
        configHadoopOptions.put("conf","dsaf");
        configHadoopOptions.put("D","sss=333");
        /*allHadoopOptions.put("fs","");
        allHadoopOptions.put("jt","");
        allHadoopOptions.put("files","");
        allHadoopOptions.put("libjars","");
        allHadoopOptions.put("archives","");*/
    }

    private static String[] haddopGenericOptionsExample1 = {"-conf", "conf file", "-Dtest=test"};
    private static String[] haddopGenericOptionsExample2 = {"-conf", "conf file", "-D", "test=test"};
    private CommandLineInterface spiedCLI;
    private Options options;
    private Option optionOne =  OptionBuilder.withLongOpt("one").withDescription("sample option one").hasArgs()
            .withArgName("Edge-Column-Name").create("1");;
    private Option optionTwo = OptionBuilder.withLongOpt("two").withDescription("sample option two").hasArgs().isRequired()
            .withArgName("Edge-Column-Name").create("2");

    @Before
    public void setUp(){
        CommandLineInterface cli = new CommandLineInterface();
        spiedCLI = spy(cli);

        options = new Options();

        options.addOption(optionOne);
        options.addOption(optionTwo);

        spiedCLI.setOptions(options);
    }

    @After
    public void tearDown(){
        spiedCLI = null;
        options = null;
    }

    @Test
    public void test_TableToGraphDB_options(){
        CommandLineInterface tableToGraphDBCLI = Whitebox.getInternalState(TableToGraphDB.class, "commandLineInterface");
        spiedCLI.setOptions(tableToGraphDBCLI.getOptions());


        HashMap<String, String> conf = new HashMap<String, String>();
        conf.put("-conf", System.getProperty("user.dir") + "files/graphbuilder.xml");
        conf.put("-D", "test=0");


        HashMap<String, String> cliArgs = new HashMap <String, String>();
        cliArgs.put("-t", "kd_sample_data");
        cliArgs.put("-v", "cf:name=cf:age,cf:dept");
        cliArgs.put("-e", "cf:name,cf:dept,worksAt");
        cliArgs.put("-a", "");
        cliArgs.put("-F", "");
        cliArgs.put("-d", "directed edge");


        for(int count = 100; count >= 0; count--){
            String[] commandLineArgs = getRandomizedCommandLine((HashMap<String,String>)conf.clone(), (HashMap<String,String>)cliArgs.clone());

            spiedCLI.checkCli(commandLineArgs);
            testRemainingArgs(10, spiedCLI);
            testParsedOptions(spiedCLI.getOptions().getOption("t"), "kd_sample_data", spiedCLI);
            testParsedOptions(spiedCLI.getOptions().getOption("v"), "cf:name=cf:age,cf:dept", spiedCLI);
            testParsedOptions(spiedCLI.getOptions().getOption("e"),"cf:name,cf:dept,worksAt", spiedCLI);
            testParsedOptions(spiedCLI.getOptions().getOption("d"),"directed edge", spiedCLI);
            assertTrue("", spiedCLI.hasOption("F"));
            assertTrue("", spiedCLI.hasOption("a"));
        }
    }

    @Test
    public void test_hadoop_generic_options_are_parsed(){
        spiedCLI.checkCli(haddopGenericOptionsExample1);

        testRemainingArgs(0, spiedCLI);
        assertEquals("verify parsed conf option", "conf file", spiedCLI.getGenericOptionsParser().getCommandLine().getOptionValue("conf"));
        assertEquals("verify parsed single option", "test=test", spiedCLI.getGenericOptionsParser().getCommandLine().getOptionValue("D"));

    }

    @Test
    public void test_hadoop_generic_options_are_parsed_reverse_order(){
        spiedCLI.checkCli(haddopGenericOptionsExample2);

        testRemainingArgs(0, spiedCLI);
        assertEquals("verify parsed conf option", "conf file", spiedCLI.getGenericOptionsParser().getCommandLine().getOptionValue("conf"));
        assertEquals("verify parsed single option", "test=test", spiedCLI.getGenericOptionsParser().getCommandLine().getOptionValue("D"));
    }

    @Test
    public void test_custom_option_parsing(){
        String[] customArgs = {"-one", "we parsed -one", "-two", "we parsed -two"};
        String[] cmdArgs = (String[]) ArrayUtils.addAll(haddopGenericOptionsExample1, customArgs);
        spiedCLI.checkCli(cmdArgs);

        testRemainingArgs(4, spiedCLI);
        testParsedOptions(optionOne, "we parsed -one", spiedCLI);
        testParsedOptions(optionTwo, "we parsed -two", spiedCLI);
        System.out.print("");
    }

    @Test
    public void test_custom_option_parsing_revers_order(){
        String[] customArgs = {"-two", "we parsed -two", "-one", "we parsed -one"};
        String[] cmdArgs = (String[]) ArrayUtils.addAll(haddopGenericOptionsExample1, customArgs);
        spiedCLI.checkCli(cmdArgs);

        testRemainingArgs(4, spiedCLI);
        testParsedOptions(optionOne, "we parsed -one", spiedCLI);
        testParsedOptions(optionTwo, "we parsed -two", spiedCLI);
        System.out.print("");
    }

    public String[] getRandomizedCommandLine(HashMap<String, String> hadoopArgs, HashMap<String, String> gbArgs){
        ArrayList<String> args = new ArrayList<String>();
        int count = 0;
        int maxCount = hadoopArgs.size()*2;
        while(count < maxCount){
            String key = getRandomKey(hadoopArgs);
            args.add(key);
            count++;
            args.add(hadoopArgs.get(key));
            count++;
            hadoopArgs.remove(key);
        }

        while(gbArgs.size() > 0 ){
            String key = getRandomKey(gbArgs);
            //args[count] = key;
            args.add(key);
            count++;
            if(!gbArgs.get(key).isEmpty()){
                //args[count] = gbArgs.get(key);
                args.add(gbArgs.get(key));
                count++;
            }
            gbArgs.remove(key);
        }

        return args.toArray(new String[args.size()]);
    }

    private String getRandomKey(HashMap<String, String> hash){
        String[] bleh = new String[hash.size()];

        int count = 0;
        for(Map.Entry<String, String> arg: hash.entrySet()){
            bleh[count] = arg.getKey();
            count++;
        }
        Random rand = new Random();

        int  n = rand.nextInt(hash.size());
        return bleh[n];
    }

    private void testParsedOptions(Option option, String expected, CommandLineInterface cli){
        if( spiedCLI.hasOption(option.getOpt()) && spiedCLI.hasOption(option.getLongOpt())){
            assertTrue("", cli.getOptionValue(option.getOpt()) == expected);
            assertTrue("", cli.getOptionValue(option.getLongOpt()) == expected);
        } else {
            fail();
        }
    }
    private void testRemainingArgs(int remaining, CommandLineInterface cli){
        assertEquals("look for remaining arguments we should have zero since we only pass hadoop parser options", remaining,
                cli.getGenericOptionsParser().getRemainingArgs().length);
    }


    private String[] getAllHadoopOptions(){
        String[] args = new String[configHadoopOptions.size() * 2];

        int count = 0;
        for(Map.Entry<String,String> option :configHadoopOptions.entrySet()){
            args[count] = "-" + option.getKey();
            count++;
            args[count] = option.getValue();
            count++;
        }
        return args;
    }
}
