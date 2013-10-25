

package com.intel.hadoop.graphbuilder.graphconstruction.inputconfiguration;

import com.intel.hadoop.graphbuilder.graphconstruction.inputmappers.TextParsingMapper;
import com.intel.hadoop.graphbuilder.util.GraphbuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
/**
 * The methods in this class are used by the full MR chain to properly configure the first MR job to work
 * with the TextParsingMapper.
 *
 * Called when setting up the first MR job of a chain,
 * it sets up the input path and input format
 *
 * @see InputConfiguration
 * @see TextParsingMapper
 *
 */
public class TextFileInputConfiguration implements InputConfiguration {

    private static final Logger LOG = Logger.getLogger(TextFileInputConfiguration.class);

    private TextInputFormat     inputFormat;
    private Class               MapperClass = TextParsingMapper.class;


    private TextFileInputConfiguration() {
    }

    public TextFileInputConfiguration(TextInputFormat inputFormat) {
        this.inputFormat = inputFormat;
    }

    public boolean usesHBase() {
        return false;
    }

    public void  updateConfigurationForMapper (Configuration configuration, CommandLine cmd)  {
    }

    public void  updateJobForMapper(Job job, CommandLine cmd) {
        job.setMapperClass(MapperClass);
        job.setInputFormatClass(inputFormat.getClass());

        String inputPath  = cmd.getOptionValue("in");

        try {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        } catch (IOException e) {
            GraphbuilderExit.graphbuilderFatalExitException(StatusCode.UNABLE_TO_LOAD_INPUT_FILE,
                    "Unable to set input path " + inputPath, LOG, e);
        }
    }

    public Class getMapperClass(){
       return MapperClass;
    }

    public void setMapperClass(Class MapperClass) {
        this.MapperClass = MapperClass;
    }

    public String getDescription() {
        return "File input format: " + inputFormat.toString();
    }
}
