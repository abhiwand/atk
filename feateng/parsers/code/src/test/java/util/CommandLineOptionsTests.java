package util;


import org.apache.commons.cli.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class CommandLineOptionsTests {
    private CommandLineOptions commandLineOptions = null;

    @Before
    public final void setUp() {
        commandLineOptions = new CommandLineOptions();
        Options options = new Options();
        OptionBuilder.hasOptionalArg();
        options.addOption(OptionBuilder.withLongOpt("input")
                .withDescription("table to read from (must exist)")
                .hasArg()
                .withArgName("input-table-name")
                .isRequired()
                .create("i"));
        commandLineOptions.setOptions(options);
    }

    @After
    public final void after() {
        commandLineOptions = null;
    }

    /**
     * see if an option ise set but not parsed
     */
    @Test
    public final void set_option_true() {
        assertTrue("check option exist", commandLineOptions.getOptions().hasOption("i"));
    }

    @Test
    public final void set_option_false() {
        assertFalse("check option doesn't exist", commandLineOptions.getOptions().hasOption("x"));
    }

    @Test
    public final void parse_args_valid_input() throws ParseException {
        String[] sampleArgs = {"-i", "input"};
        CommandLine cmd = commandLineOptions.parseArgs(sampleArgs);
        assertTrue("parse args verify options are set", cmd.hasOption("i"));
    }

    @Test(expected = UnrecognizedOptionException.class)
    public final void parse_args_invalid_input() throws ParseException {
        String[] sampleArgs = {"-x", "input"};
        commandLineOptions.parseArgs(sampleArgs);
    }

    @Test
    public final void has_option_valid_input() throws ParseException {
        String[] sampleArgs = {"-i", "input"};
        commandLineOptions.parseArgs(sampleArgs);
        assertTrue("check for a valid option", commandLineOptions.hasOption("i"));
    }

    @Test
    public final void has_option_invalid_input() throws ParseException {
        String[] sampleArgs = {"-i", "input"};
        commandLineOptions.parseArgs(sampleArgs);
        assertFalse("check for invalid option", commandLineOptions.hasOption("x"));
    }

    @Test
    public final void get_option_value_valid_input() throws ParseException {
        String[] sampleArgs = {"-i", "input"};
        commandLineOptions.parseArgs(sampleArgs);
        assertEquals("get a valid option value", "input", commandLineOptions.getOptionValue("i"));
    }

    @Test
    public final void get_option_value_invalid_input() throws ParseException {
        String[] sampleArgs = {"-i", "input"};
        commandLineOptions.parseArgs(sampleArgs);
        assertEquals("get an invalid option value", null, commandLineOptions.getOptionValue("x"));
    }

    @Test
    public final void look_for_option_exception_missing_option_true() throws ParseException {
        String[] sampleArgs = {"i"};
        CommandLine cmd = null;
        try {
            cmd = commandLineOptions.parseArgs(sampleArgs);
        } catch (ParseException e) {
            assertTrue("i should me the missing option", CommandLineOptions.lookForOptionException(e, "i"));
        }
        if (cmd != null) fail("didn't throw exception");
    }

    @Test
    public final void look_for_option_exception_missing_option_false() throws ParseException {
        String[] sampleArgs = {"-i", "input"};
        commandLineOptions.setOption(OptionBuilder.withLongOpt("output")
                .withDescription("table to write to (must exist)")
                .hasArg()
                .withArgName("input-table-name")
                .isRequired()
                .create("o"));
        CommandLine cmd = null;
        try {
            cmd = commandLineOptions.parseArgs(sampleArgs);
        } catch (ParseException e) {
            assertFalse("we should not have i as a missing option", CommandLineOptions.lookForOptionException(e, "i"));
        }
        if (cmd != null) fail("didn't throw exception");
    }
}
