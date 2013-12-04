/* Copyright (C) 2013 Intel Corporation.
*     All rights reserved.
*
 *  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*
* For more about this software visit:
*      http://www.01.org/GraphBuilder
 */

package com.intel.hadoop.graphbuilder.pipeline.input.text;

import com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
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
 * @see com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration
 * @see TextParsingMapper
 *
 */
public class TextFileInputConfiguration implements InputConfiguration {

    private static final Logger LOG = Logger.getLogger(TextFileInputConfiguration.class);

    private TextInputFormat inputFormat;
    private Class           MapperClass = TextParsingMapper.class;
    private Path            inputPath;


    public TextFileInputConfiguration(TextInputFormat inputFormat, String inputPathString) {
        this.inputFormat = inputFormat;
        this.inputPath   = new Path(inputPathString);

    }

    public boolean usesHBase() {
        return false;
    }

    public void  updateConfigurationForMapper (Configuration configuration)  {
    }

    public void  updateJobForMapper(Job job) {
        job.setMapperClass(MapperClass);
        job.setInputFormatClass(inputFormat.getClass());

        try {
            FileInputFormat.addInputPath(job, inputPath );
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNABLE_TO_LOAD_INPUT_FILE,
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
