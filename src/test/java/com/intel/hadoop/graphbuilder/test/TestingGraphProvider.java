package com.intel.hadoop.graphbuilder.test;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * Get a TitanGraph backed by Berkeley for testing purposes.
 *
 * This is like a factory but make sure to call cleanUp() when you are done with
 * it to remove any temporary files.
 */
public class TestingGraphProvider {

    private static final Logger LOG = Logger.getLogger(TestingGraphProvider.class);

    private TitanGraph graph;
    private File tmpDir;

    /**
     * Get a TitanGraph backed by Berkeley for testing purposes.
     *
     * Make sure to call cleanUp() when you are done with it.
     */
    public TitanGraph getTitanGraph() {
        if (graph == null) {
            createTempDirectory();
            graph = TitanFactory.open(tmpDir.getAbsolutePath());
        }
        return graph;
    }

    /**
     * IMPORTANT!
     *
     * Call cleanUp() when you are done to remove temporary files
     */
    public void cleanUp() {
        graph.shutdown();
        deleteTempDirectory();

        graph = null;
        tmpDir = null;
    }

    private void createTempDirectory() {
        try {
            tmpDir = File.createTempFile("titan-graph-for-unit-testing-", "-tmp");
            tmpDir.delete(); // convert file to a directory
            if (!tmpDir.mkdirs()) {
                LOG.error("Failed to create tmpDir: " + tmpDir.getAbsolutePath());
            }
        }
        catch(IOException e) {
            throw new RuntimeException("Could NOT initialize TestingGraphProvider", e);
        }
    }

    private void deleteTempDirectory() {
        FileUtils.deleteQuietly(tmpDir);
        if (tmpDir.exists()) {
            LOG.error("Failed to delete tmpDir: " + tmpDir.getAbsolutePath());
        }

    }
}
