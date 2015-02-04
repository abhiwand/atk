##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
"""
Results post-processing
"""

# This is a stop-gap solution for post-processing results from commands whose
# definitions are dynamically loaded from the server


def get_postprocessor(command_full_name):
    """Look for a result post-processing function, returns None if not found"""
    return _postprocessors.get(command_full_name, None)


# command-> post-processor table, populated by decorator
_postprocessors = {}


# decorator
def postprocessor(*command_full_names):
    def _postprocessor(function):
        for name in command_full_names:
            add_postprocessor(name, function)
        return function
    return _postprocessor


def add_postprocessor(command_full_name, function):
    if command_full_name in _postprocessors:
        raise RuntimeError("Internal Error: duplicate command name '%s' in results post-processors" % command_full_name)
    _postprocessors[command_full_name] = function

def add_return_none_postprocessor(command_full_name):
    add_postprocessor(command_full_name, return_none)


# post-processor methods --all take a json object argument

@postprocessor('graph:titan/sampling/vertex_sample', 'graph:/export_to_titan', 'graph:titan/export_to_graph',
               'graph:titan/annotate_degrees', 'graph:titan/annotate_weighted_degrees', 'graph/copy')
def return_graph(selfish, json_result):

    from intelanalytics.core.graph import get_graph
    return get_graph(json_result['id'])

@postprocessor('frame/classification_metrics', 'model:logistic_regression/test', 'model:svm/test')
def return_metrics(selfish, json_result):
     from intelanalytics.core.classifymetrics import ClassificationMetricsResult
     return ClassificationMetricsResult(json_result)

@postprocessor('frame/tally', 'frame/tally_percent', 'frame/cumulative_sum', 'frame/cumulative_percent', 'frame:/drop_columns', 'frame/bin_column', 'frame/drop_duplicates', 'frame/flatten_column')
def return_none(selfish, json_result):
    return None

@postprocessor('frame/histogram')
def return_histogram(selfish, json_result):
    from intelanalytics.core.histogram import Histogram
    return Histogram(json_result["cutoffs"], json_result["hist"], json_result["density"])

@postprocessor('graph:titan/clustering_coefficient')
def return_clustering_coefficient(selfish, json_result):
    from intelanalytics.core.graph import get_graph
    from intelanalytics.core.clusteringcoefficient import  ClusteringCoefficient
    return ClusteringCoefficient(json_result['global_clustering_coefficient'], get_graph(json_result['graph']['id']))

@postprocessor('frame/bin_column_equal_depth', 'frame/bin_column_equal_width')
def return_bin_result(selfish, json_result):
    selfish._id = json_result['frame']['id']
    return json_result["cutoffs"]

