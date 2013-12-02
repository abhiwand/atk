package com.intel.hadoop.graphbuilder.util;

import org.apache.commons.cli.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * A general command line parsing utility for graph builder.
 *
 * <p>
 *     <code>-conf</code>   specifies the configuration file.
 * </p>
 */
public class CommandLineInterface {

    private static final Logger  LOG           = Logger.getLogger(CommandLineInterface.class);
    private static final String  GENERIC_ERROR = "Error parsing options";
    private Options              options       = new Options();
    private CommandLine          cmd           = null;
    private RuntimeConfig        runtimeConfig = RuntimeConfig.getInstance();
    private GenericOptionsParser genericOptionsParser;

    /**
     * Does this command line have the specified option?
     * @param option  The name of option being requested.
     * @return  true  If the command line has the option.
     */
    public boolean hasOption(String option) {
        return cmd.hasOption(option);
    }

    /**
     * Gets the value of the option from the command line.
     * @param option The name of option whose value is requested.
     * @return value The value of the option as specified by the command line.
     */
    public String getOptionValue(String option) {
        return cmd.getOptionValue(option);
    }

    /**
     * Parses the raw arguments into the {@code CommandLine} object.
     * @param args The raw command line arguments as a string array.
     * @return  A nicely packaged {@code CommandLine} object.
     */
    public CommandLine parseArgs(String[] args) {

        for (int i = 0; i < args.length;i++) {
            if (args[i].equals("-conf")) {
                if (i + 1 == args.length) {
                    GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                            "-conf argument given but no file path specified!", LOG);
                } else if (!new File(args[i+1]).exists()) {
                    GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.CANNOT_FIND_CONFIG_FILE,
                            "Configuration file " + args[i+1] + " cannot be found.", LOG);
                }
            }
        }

        //send the command line options to hadoop parse args to get runtime config options first

        try {
            genericOptionsParser = new GenericOptionsParser(args);
        } catch (IOException e) {
            // show help and terminate the process
            showHelp("Error parsing hadoop generic options.");
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
                showHelpMissingOption(getUnrecognizedOptionFromException(e));

            }else if(e instanceof MissingOptionException){
                showHelpOption(getFirstMissingOptionFromException(e));
            }else if(e instanceof MissingArgumentException){
                showHelpMissingArgument(getMissingArgumentFromException(e));
            } else {
                showHelp("Error parsing option string.");
            }
        }
        return cmd;
    }

    /**
     * Makes sure that all required options are present in the raw arguments.
     * @param args  The raw arguments as a string array.
     */
    public void checkCli(String[] args) {
        parseArgs(args);
        options.getRequiredOptions().iterator();
        List<String> opts = options.getRequiredOptions();
        for( String option: opts){
            if (!cmd.hasOption(option)) {
                showHelpOption(option);
            }
            else {
                showOptionParsed(option);
            }
        }
    }

    /**
     * Gets Hadoop's generic options parser.
     * @return  Hadoop's generic options parser.
     */
    public GenericOptionsParser getGenericOptionsParser() {
        return genericOptionsParser;
    }

    /**
     * Displays the parsed options for the given option name.
     * @param option The name of option as a string.
     */
    public void showOptionParsed(String option){
        LOG.info(String.format("%s: %s", options.getOption(option).getLongOpt(), cmd.getOptionValue(option) ));
    }

    /**
     * Displays the parsed options.
     */
    public void showOptionsParsed(){
        Iterator optionss = options.getOptions().iterator();
        while( optionss.hasNext()){
            Option toPrint = (Option) optionss.next();
            if (cmd.hasOption(toPrint.getOpt())) {
                showOptionParsed(toPrint.getOpt());
            }
        }
    }

    /**
     * Displays help after an error message.
     * @param message  The error message.
     */
    public void showHelp(String message){
        _showHelp(message);
    }

    private void showHelpMissingArgument(String option){
        String error = GENERIC_ERROR;
        if( option != null){
            error = String.format("Option -%s --%s %s is missing it's argument", options.getOption(option).getOpt(),
                    options.getOption(option).getLongOpt(), options.getOption(option).getDescription());
        }
        _showHelp(error);
    }

    private void showHelpMissingOption(String option){
        String error = GENERIC_ERROR;
        if( option != null){
            error = String.format("Option -%s not recognized", option);
        }
        _showHelp(error);
    }

    private void showHelpOption(String option) {
        String error = GENERIC_ERROR;
        if( option != null){
            error = String.format("Option -%s --%s %s is missing", options.getOption(option).getOpt(),
                    options.getOption(option).getLongOpt(), options.getOption(option).getDescription());
        }
        _showHelp(error);
    }

    private void _showHelp(String error){
        if(error.trim().length() > 0){
            LOG.fatal(error);
        }
        HelpFormatter h = new HelpFormatter();
        h.printHelp(error, options);
        GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                "Unable to process command line.", LOG);
    }

    public void setOptions(Options options) {
        this.options = options;
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
     * Checks if the lack of an option caused a parsing exception.
     * @param e      The parse exception that was thrown.
     * @param option The option that should be in the MissingOptionException.
     * @return A boolean indicating weather or not the String option is the missing option for which we are looking.
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
     * Converts the missing argument exception into a string message.
     * @param ex A ParseException.
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
     * Checks if an unrecognized option caused a parsing exception.
     *
     * @param ex The parsing exception.
     * @return The name of the unrecognized option.
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
     * Finds the first missing option from a parsing exception.
     * @param ex The parsing exception.
     * @return  The name of the first missing option.
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
