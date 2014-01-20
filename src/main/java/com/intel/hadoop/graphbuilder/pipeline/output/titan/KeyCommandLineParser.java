package com.intel.hadoop.graphbuilder.pipeline.output.titan;

import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses the command line -keys option into a list of GBTitanKey objects.
 */
public class KeyCommandLineParser {

    private static final Logger LOG = Logger.getLogger(KeyCommandLineParser.class);

    /*
     * This method does the parsing of the command line -keys option
     * into a list of GBTitanKey objects.
     *
     * The -keys option takes a comma separated list of key rules.
     *
     * A key rule is:  <property name>;<option_1>; ... <option_n>
     * where the options are datatype specifiers, flags to use the key for
     * indexing edges and vertices, or a uniqueness bit,
     * per the definitions in TitanCommandLineOptions.
     *
     * Example:
     *    -keys  cf:userId;String;U;V,cf:eventId;E;Long
     *
     *    Generates a key for property cf:UserId that is a unique vertex
     *    index taking string values, and a key for property cf:eventId that
     *    is an edge index taking Long values.
     */
    public List<GBTitanKey> parse(String keyCommandLine) {

        ArrayList<GBTitanKey> gbKeyList = new ArrayList<GBTitanKey>();

        if (keyCommandLine.length() > 0) {

            String[] keyRules = keyCommandLine.split("\\,");

            for (String keyRule : keyRules) {
                String[] ruleProperties = keyRule.split(";");

                if (ruleProperties.length > 0) {
                    String propertyName = ruleProperties[0];

                    GBTitanKey gbTitanKey = new GBTitanKey(propertyName);

                    for (int i = 1; i < ruleProperties.length; i++) {
                        String ruleModifier = ruleProperties[i];

                        if (ruleModifier.equals(TitanCommandLineOptions
                                .STRING_DATATYPE)) {
                            gbTitanKey.setDataType(String.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.INT_DATATYPE)) {
                            gbTitanKey.setDataType(Integer.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.LONG_DATATYPE)) {
                            gbTitanKey.setDataType(Long.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.DOUBLE_DATATYPE)) {
                            gbTitanKey.setDataType(Double.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.FLOAT_DATATYPE)) {
                            gbTitanKey.setDataType(Float.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.VERTEX_INDEXING)) {
                            gbTitanKey.setIsVertexIndex(true);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.EDGE_INDEXING)) {
                            gbTitanKey.setIsEdgeIndex(true);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.UNIQUE)) {
                            gbTitanKey.setIsUnique(true);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.NOT_UNIQUE)) {
                            gbTitanKey.setIsUnique(false);
                        } else {
                            GraphBuilderExit.graphbuilderFatalExitNoException
                                    (StatusCode.BAD_COMMAND_LINE,
                                            "Error declaring keys.  " + ruleModifier
                                                    + " is not a valid option.\n" +
                                                    TitanCommandLineOptions
                                                            .KEY_DECLARATION_CLI_HELP,
                                            LOG);
                        }
                    }

                    // Titan requires that unique properties be vertex indexed
                    if (gbTitanKey.isUnique()) {
                        gbTitanKey.setIsVertexIndex(true);
                    }

                    gbKeyList.add(gbTitanKey);
                }
            }
        }

        return gbKeyList;
    }
}
