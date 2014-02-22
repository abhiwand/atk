package com.intel.graph;

import org.apache.hadoop.fs.Path;

/**
 * Interface which encapsulates method for collecting path
 */
public interface IPathCollector {

    /**
     * Collect path
     * @param path: The path to be collected
     */
    void collectPath(Path path);
}
