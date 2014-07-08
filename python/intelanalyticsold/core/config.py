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


# Frame
def get_frame_backend():
    global _frame_backend
    if _frame_backend is None:
        from intelanalytics.rest.frame import FrameBackendRest
        _frame_backend = FrameBackendRest()
    return _frame_backend
_frame_backend = None

# Graph
def get_graph_backend():
    global _graph_backend
    if _graph_backend is None:
        from intelanalytics.rest.graph import GraphBackendRest
        _graph_backend = GraphBackendRest()
    return _graph_backend
_graph_backend = None
