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
Post Processing of clustering_coefficient result
"""
class ClusteringCoefficient:
    """
    Class containing the results of a clustering coefficient operation.

     Members
     --------
     global_clustering_coefficient : Double
        cutoff points of each bin. There are n-1 bins in the result
     graph : Graph
        a copy of the input graph with the local clustering coefficients annotated to each vertex
        will not be present when only global clustering coefficient is requested
    """
    def __init__(self, global_clustering_coefficient, graph):
        self.global_clustering_coefficient = global_clustering_coefficient
        self.graph = graph

    def __repr__(self):
        return """ClusteringCoefficient:
global_clustering_coefficient: %s,
graph: %s""" % (self.global_clustering_coefficient, self.graph)
