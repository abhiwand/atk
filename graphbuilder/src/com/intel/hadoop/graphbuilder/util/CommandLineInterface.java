package com.intel.hadoop.graphbuilder.util;

import com.intel.hadoop.graphbuilder.pipeline.GraphConstructionPipeline;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * General command line parsing utility for graph builder. Uses the Hadoop generic options parser to parse config files.
 * reserved options -conf, -D, -fs, -jt, -files, -libjars, -archives already used by the Hadoop generic options parser.
 * don't use any of the reserved options to avoid conflicts.
 *
 * <p>
 *     <code>-conf path/to/config/file</code>   specifies the configuration file
 *     <code>-DmySingleConfigName=mySingleConfigValue</code>   specifies the configuration file
 *     or
 *     <code>-D mySingleConfigName=mySingleConfigValue</code>   specifies the configuration file
 * </p>
 */
public class CommandLineInterface{

    private static final Logger  LOG           = Logger.getLogger(CommandLineInterface.class);
    private static final String  GENERIC_ERROR = "Error parsing options";
    private static final Option HELP_OPTION = OptionBuilder.withLongOpt("help").withDescription("").create("h");
    private Options              options       = new Options();
    private CommandLine          cmd           = null;
    private RuntimeConfig        runtimeConfig = RuntimeConfig.getInstance();
    private GenericOptionsParser genericOptionsParser;


    /**
     * wrapper to the regular hasOption command line class
     * does this command line have the specified option?
     * @param option  name of option being requested
     * @return  true iff the command line has the option
     */
    public boolean hasOption(String option) {
        return cmd.hasOption(option);
    }

    /**
     * wrapper to the regular getOptionValue command line class
     * Get value of option from command line
     * @param option name of option whose value is requested
     * @return value of the option as specified by the command line
     */
    public String getOptionValue(String option) {
        return cmd.getOptionValue(option);
    }

    public GraphConstructionPipeline addConfig(GraphConstructionPipeline job){
        return this.getRuntimeConfig().addConfig(job);
    }

    /**
     * Parse raw arguments into {@code CommandLine} object
     * @param args raw command line arguments as string array
     * @return  nicely packaged {@code CommandLine} object
     */
    public CommandLine parseArgs(String[] args) {

        //send the command line options through the hadoop parser the config options first

        try {
            genericOptionsParser = new GenericOptionsParser(args);
        } catch (IOException e) {
            // show help and terminate the process
            showHelp("Error parsing hadoop generic options.");
        }

        //make sure the config file exist when it's specified
        if(genericOptionsParser.getCommandLine().hasOption("conf") &&
                !new File(genericOptionsParser.getCommandLine().getOptionValue("conf")).exists()){
            GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.CANNOT_FIND_CONFIG_FILE,
                    "Configuration file " + genericOptionsParser.getCommandLine().getOptionValue("conf") +
                            " cannot be found.", LOG);
        }

        //load all the grahpbuilder configs into the runtime class

        runtimeConfig.loadConfig(genericOptionsParser.getConfiguration());

        //parse the remaining args

        CommandLineParser parser = new PosixParser();

        try {
            cmd = parser.parse(options, genericOptionsParser.getRemainingArgs());
        }
        catch (ParseException e){
            if(e instanceof UnrecognizedOptionException){
                showErrorUnrecognizedOption(getUnrecognizedOptionFromException(e));

            }else if(e instanceof MissingOptionException){
                showErrorOption(getFirstMissingOptionFromException(e));

            }else if(e instanceof MissingArgumentException){
                showErrorMissingArgument(getMissingArgumentFromException(e));

            } else {
                showHelp("Error parsing option string.");
            }
        }
        return cmd;
    }

    /**
     * Make sure that all required options are present in raw arguments..
     * @param args  raw arguments as string array
     */
    public CommandLine checkCli(String[] args) {
        CommandLine cmd = parseArgs(args);

        if (cmd == null) {
            showHelp("Error parsing command line options");
            GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                    "Error parsing command line options", LOG);
        }

        if(cmd.hasOption(HELP_OPTION.getOpt())){
            showHelp("Help1");
        }

        //report any missing required options
        List<String> opts = options.getRequiredOptions();
        for(String option: opts){
            if (!cmd.hasOption(option)) {
                showErrorOption(option);
            }
        }

        //report parsed values for options given
        showOptionsParsed();
        return cmd;
    }

    /**
     * Get Hadoop's generic options parser
     * @return  Hadoop's generic options parser
     */
    public GenericOptionsParser getGenericOptionsParser() {
        return genericOptionsParser;
    }

    /**
     * Displays parsed options given option name.
     * @param option name of option as string
     */
    public void showParsedOption(Option option){
        String message;
        if(option.hasArg()){
            message = String.format("Parsed -%s -%s:\t %s", option.getOpt(),
                    option.getLongOpt(), cmd.getOptionValue(option.getOpt()) );
        }else{
            message = String.format("Parsed -%s -%s:\t %b", option.getOpt(),
                    option.getLongOpt(), cmd.hasOption(option.getOpt()) );
        }
        LOG.info(message);
    }

    /**
     * Display parsed options.
     */
    public void showOptionsParsed(){
        Iterator<Option> optionIterator = options.getOptions().iterator();
        while(optionIterator.hasNext()){
            Option nextOption = optionIterator.next();
            if (cmd.hasOption(nextOption.getOpt())) {
                showParsedOption(nextOption);
            }
        }
    }

    /**
     * Display help message when users sets the help option
     * @param message  error message
     */
    public void showHelp(String message){
        _showHelp(message);
    }

    /**
     * Display help message after a bad command line param
     * @param message error message to display on the command line
     */
    public void showError(String message){
        _showError(message);
    }

    private void showErrorMissingArgument(String option){
        String error = GENERIC_ERROR;
        if( option != null){
            error = String.format("Option -%s --%s %s is missing it's argument", options.getOption(option).getOpt(),
                    options.getOption(option).getLongOpt(), options.getOption(option).getDescription());
        }
        _showError(error);
    }

    private void showErrorUnrecognizedOption(String option){
        String error = GENERIC_ERROR;
        if( option != null){
            error = String.format("Option -%s not recognized", option);
        }
        _showError(error);
    }

    private void showErrorOption(String option) {
        String error = GENERIC_ERROR;
        if( option != null){
            //show the short, long option and the description for the missing option
            error = String.format("Option -%s --%s %s is missing", options.getOption(option).getOpt(),
                    options.getOption(option).getLongOpt(), options.getOption(option).getDescription());
        }
        _showError(error);
    }

    private void _showError(String error){
        if(error == null || error.trim().length() > 0){
            error = " ";
        }
        HelpFormatter h = new HelpFormatter();
        h.printHelp(error, options);
        GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                "Unable to process command line.", LOG);
    }

    private void _showHelp(String help){
        if(help == null || help.trim().length() > 0){
            help = " ";
        }
        HelpFormatter h = new HelpFormatter();
        h.printHelp(help, options);
        GraphBuilderExit.graphbuilderExitNoException(StatusCode.SUCCESS);
    }

    public void setOptions(Options options) {
        this.options = options;
        this.options.addOption(HELP_OPTION);
    }

    public void removeOptions() {
        this.options = null;
    }

    public void setOption(Option option) {
        this.options.addOption(option);
    }


    public RuntimeConfig getRuntimeConfig() {
        return runtimeConfig;
    }

    public Options getOptions() {
        return options;
    }

    public CommandLine getCmd() {
        return cmd;
    }

    /**
     * Check if the lack of an option caused a parsing exception
     * @param e      the parse exception that was thrown
     * @param option the option that should be in the MissingOptionException
     * @return a boolean on weather or not the String option is the missing option we are looking for
     */
    public static boolean lookForOptionException(ParseException e, String option) {
        MissingOptionException missingOptions = (MissingOptionException) e;

        for (int index = 0; index < missingOptions.getMissingOptions().size(); index++) {

            String checkOption = (String) missingOptions.getMissingOptions().get(index);

            if (checkOption.equals(option)) return true;
        }
        return false;
    }

    /**
     * Convert missing argument exception into string message.
     * @param ex a ParseException
     */
    public static String getMissingArgumentFromException(ParseException ex){
        MissingArgumentException missingArgumentException;

        try{
            missingArgumentException = (MissingArgumentException) ex;
        } catch (ClassCastException e){
            return null;
        }

        if(missingArgumentException.getOption() != null ){
            return missingArgumentException.getOption().getOpt();
        } else {
            return null;
        }
    }

    /**
     * Check if an unrecognized option caused a parsing exception.
     *
     * @param ex the parsing exception
     * @return name of unrecognized option
     */
    public static String getUnrecognizedOptionFromException(ParseException ex){
        UnrecognizedOptionException unrecognizedOption;

        try{
            unrecognizedOption = (UnrecognizedOptionException) ex;
        } catch (ClassCastException e){
            return null;
        }

        if(unrecognizedOption.getOption() != null ){
            return unrecognizedOption.getOption();
        } else {
            return null;
        }
    }

    /**
     * Find the first missing option from a parsing exception
     * @param ex the parsing exception
     * @return  name of the first missing option
     */
    public static String getFirstMissingOptionFromException(ParseException ex){
        MissingOptionException missingOptions;

        try{
            missingOptions = (MissingOptionException) ex;
        } catch (ClassCastException e){
            return null;
        }

        if(missingOptions.getMissingOptions().size() > 0 ){
            return (String) missingOptions.getMissingOptions().get(0);
        }
        else{
            return null;
        }

    }
}
