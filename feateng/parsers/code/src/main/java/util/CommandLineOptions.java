package util;

import org.apache.commons.cli.*;

/**
 * A very simple util class to remove some of the command line parsing from the mapper class to make it easier to test
 */
public class CommandLineOptions {
    private Options options = new Options();
    private CommandLine cmd = null;

    public boolean hasOption(String option) {
        return cmd.hasOption(option);
    }

    public String getOptionValue(String option) {
        return cmd.getOptionValue(option);
    }

    public CommandLine parseArgs(String[] args) throws ParseException {
        CommandLineParser parser = new PosixParser();
        cmd = parser.parse(options, args);
        return cmd;
    }

    public void setOptions(Options options) {
        this.options = options;
    }

    public void setOption(Option option) {
        this.options.addOption(option);
    }

    public Options getOptions() {
        return options;
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
}
