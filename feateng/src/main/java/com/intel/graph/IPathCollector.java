package com.intel.graph;

import org.apache.hadoop.fs.Path;

public interface IPathCollector {
    void collectPath(Path path);
}
