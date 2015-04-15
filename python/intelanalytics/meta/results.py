##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
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
    """
    Look for a result post-processing function.
    Returns None if not found.
    """
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

@postprocessor('frame/tally', 'frame/tally_percent', 'frame/cumulative_sum', 'frame/cumulative_percent', 'frame:/drop_columns',
               'frame/bin_column', 'frame/drop_duplicates', 'frame/flatten_column', 'graph:titan/sampling/assign_sample')
def return_none(selfish, json_result):
    return None

@postprocessor('frame/histogram')
def return_histogram(selfish, json_result):
    from intelanalytics.core.histogram import Histogram
    return Histogram(json_result["cutoffs"], json_result["hist"], json_result["density"])

@postprocessor('graph/clustering_coefficient')
def return_clustering_coefficient(selfish, json_result):
    from intelanalytics.core.frame import get_frame
    from intelanalytics.core.clusteringcoefficient import  ClusteringCoefficient
    if json_result.has_key('frame'):
        frame = get_frame(json_result['frame']['id'])
    else:
        frame = None
    return ClusteringCoefficient(json_result['global_clustering_coefficient'], frame)

@postprocessor('frame/bin_column_equal_depth', 'frame/bin_column_equal_width')
def return_bin_result(selfish, json_result):
    selfish._id = json_result['frame']['id']
    return json_result["cutoffs"]

@postprocessor('model:lda/train')
def return_lda_train(selfish, json_result):
    from intelanalytics.core.frame import get_frame
    doc_frame = get_frame(json_result['doc_results']['id'])
    word_frame= get_frame(json_result['word_results']['id'])
    return { 'doc_results': doc_frame, 'word_results': word_frame, 'report': json_result['report'] }

@postprocessor('graph/graphx_connected_components','graph/annotate_weighted_degrees','graph/annotate_degrees')
def return_connected_components(selfish, json_result):
    from intelanalytics.core.frame import get_frame
    dictionary = json_result["frame_dictionary_output"]
    return dict([(k,get_frame(v["id"])) for k,v in dictionary.items()])

@postprocessor('graph/graphx_page_rank')
def return_page_ank(selfish, json_result):
    from intelanalytics.core.frame import get_frame
    vertex_json = json_result["vertex_dictionary_output"]
    edge_json = json_result["edge_dictionary_output"]
    vertex_dictionary = dict([(k,get_frame(v["id"])) for k,v in vertex_json.items()])
    edge_dictionary = dict([(k,get_frame(v["id"])) for k,v in edge_json.items()])
    return {'vertex_dictionary': vertex_dictionary, 'edge_dictionary': edge_dictionary}