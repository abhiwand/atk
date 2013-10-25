package com.intel.hadoop.graphbuilder.util;

import org.apache.commons.cli.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * A simple util class to remove some of the command line parsing from the mapper class to make it easier to test
 */
public class CommandLineInterface {

    private static final Logger  LOG           = Logger.getLogger(CommandLineInterface.class);
    private static final String  GENERIC_ERROR = "Error parsing options";
    private Options              options       = new Options();
    private CommandLine          cmd           = null;
    private RuntimeConfig        runtimeConfig = RuntimeConfig.getInstance();
    private GenericOptionsParser genericOptionsParser;

    public boolean hasOption(String option) {
        return cmd.hasOption(option);
    }

    public String getOptionValue(String option) {
        return cmd.getOptionValue(option);
    }

    public CommandLine parseArgs(String[] args) {

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


    public GenericOptionsParser getGenericOptionsParser() {
        return genericOptionsParser;
    }

    public void showOptionParsed(String option){
        LOG.info(String.format("%s: %s", options.getOption(option).getLongOpt(), cmd.getOptionValue(option) ));
    }

    public void showOptionsParsed(){
        Iterator optionss = options.getOptions().iterator();
        while( optionss.hasNext()){
            Option toPrint = (Option) optionss.next();
            if (cmd.hasOption(toPrint.getOpt())) {
                showOptionParsed(toPrint.getOpt());
            }
        }
    }
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
        GraphbuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
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
