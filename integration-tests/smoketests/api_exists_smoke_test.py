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

import unittest
import intelanalytics as ia

# show full stack traces
ia.errors.show_details = True
ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()

class ApiExistsSmokeTest(unittest.TestCase):
    """
    This test makes sure the API exists.  Sometimes packaging or plugin system bugs might cause
    parts of the API to disappear.  This helps catch it quickly.

    ---

    Smoke test basic frame operations to verify functionality that will be needed by all other tests.

    If these tests don't pass, there is no point in running other tests.

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_expected_methods_exist_on_csvfile(self):
        self.assert_method_defined('annotation', ia.CsvFile)
        self.assert_method_defined('field_names', ia.CsvFile)
        self.assert_method_defined('field_types', ia.CsvFile)

    def test_expected_methods_exist_on_frame(self):
        self.assert_method_defined('add_columns', ia.Frame)
        self.assert_method_defined('append', ia.Frame)
        self.assert_method_defined('assign_sample', ia.Frame)
        self.assert_method_defined('bin_column', ia.Frame)
        self.assert_method_defined('bin_column_equal_depth', ia.Frame)
        self.assert_method_defined('bin_column_equal_width', ia.Frame)
        self.assert_method_defined('classification_metrics', ia.Frame)
        self.assert_method_defined('column_median', ia.Frame)
        self.assert_method_defined('column_mode', ia.Frame)
        self.assert_method_defined('column_names', ia.Frame)
        self.assert_method_defined('column_summary_statistics', ia.Frame)
        self.assert_method_defined('copy', ia.Frame)
        self.assert_method_defined('correlation', ia.Frame)
        self.assert_method_defined('correlation_matrix', ia.Frame)
        self.assert_method_defined('count', ia.Frame)
        self.assert_method_defined('covariance', ia.Frame)
        self.assert_method_defined('covariance_matrix', ia.Frame)
        self.assert_method_defined('cumulative_percent', ia.Frame)
        self.assert_method_defined('cumulative_sum', ia.Frame)
        self.assert_method_defined('dot_product', ia.Frame)
        self.assert_method_defined('download', ia.Frame)
        self.assert_method_defined('drop_columns', ia.Frame)
        self.assert_method_defined('drop_duplicates', ia.Frame)
        self.assert_method_defined('drop_rows', ia.Frame)
        self.assert_method_defined('ecdf', ia.Frame)
        self.assert_method_defined('entropy', ia.Frame)
        self.assert_method_defined('export_to_csv', ia.Frame)
        self.assert_method_defined('export_to_json', ia.Frame)
        self.assert_method_defined('filter', ia.Frame)
        self.assert_method_defined('flatten_column', ia.Frame)
        self.assert_method_defined('get_error_frame', ia.Frame)
        self.assert_method_defined('group_by', ia.Frame)
        self.assert_method_defined('histogram', ia.Frame)
        self.assert_method_defined('inspect', ia.Frame)
        self.assert_method_defined('join', ia.Frame)
        self.assert_method_defined('name', ia.Frame)
        self.assert_method_defined('quantiles', ia.Frame)
        self.assert_method_defined('rename_columns', ia.Frame)
        self.assert_method_defined('row_count', ia.Frame)
        self.assert_method_defined('schema', ia.Frame)
        self.assert_method_defined('sort', ia.Frame)
        self.assert_method_defined('status', ia.Frame)
        self.assert_method_defined('take', ia.Frame)
        self.assert_method_defined('tally', ia.Frame)
        self.assert_method_defined('tally_percent', ia.Frame)
        self.assert_method_defined('top_k', ia.Frame)

    def test_expected_methods_exist_on_graph(self):
        self.assert_method_defined('annotate_degrees', ia.Graph)
        self.assert_method_defined('annotate_weighted_degrees', ia.Graph)
        self.assert_method_defined('clustering_coefficient', ia.Graph)
        self.assert_method_defined('copy', ia.Graph)
        self.assert_method_defined('define_edge_type', ia.Graph)
        self.assert_method_defined('define_vertex_type', ia.Graph)
        self.assert_method_defined('edge_count', ia.Graph)
        self.assert_method_defined('edges', ia.Graph)
        self.assert_method_defined('export_to_titan', ia.Graph)
        self.assert_method_defined('graphx_connected_components', ia.Graph)
        self.assert_method_defined('graphx_pagerank', ia.Graph)
        self.assert_method_defined('graphx_triangle_count', ia.Graph)
        self.assert_method_defined('ml', ia.Graph)
        self.assert_method_defined('name', ia.Graph)
        self.assert_method_defined('status', ia.Graph)
        self.assert_method_defined('vertex_count', ia.Graph)
        self.assert_method_defined('vertices', ia.Graph)

    def test_expected_methods_exist_on_titangraph(self):
        self.assert_method_defined('annotate_degrees', ia.TitanGraph)
        self.assert_method_defined('annotate_weighted_degrees', ia.TitanGraph)
        self.assert_method_defined('append', ia.TitanGraph)
        self.assert_method_defined('clustering_coefficient', ia.TitanGraph)
        self.assert_method_defined('connected_components', ia.TitanGraph)
        self.assert_method_defined('copy', ia.TitanGraph)
        self.assert_method_defined('export_to_graph', ia.TitanGraph)
        self.assert_method_defined('graphx_connected_components', ia.TitanGraph)
        self.assert_method_defined('graphx_pagerank', ia.TitanGraph)
        self.assert_method_defined('graphx_triangle_count', ia.TitanGraph)
        self.assert_method_defined('hierarchical_clustering', ia.TitanGraph)
        self.assert_method_defined('load', ia.TitanGraph)
        self.assert_method_defined('ml', ia.TitanGraph)
        self.assert_method_defined('name', ia.TitanGraph)
        self.assert_method_defined('page_rank', ia.TitanGraph)
        self.assert_method_defined('query', ia.TitanGraph)
        self.assert_method_defined('sampling', ia.TitanGraph)
        self.assert_method_defined('status', ia.TitanGraph)

    def test_expected_methods_exist_on_kmeansmodel(self):
        self.assert_method_defined("name", ia.KMeansModel)
        self.assert_method_defined("predict", ia.KMeansModel)
        self.assert_method_defined("train", ia.KMeansModel)

    def test_expected_methods_exist_on_ldamodel(self):
        self.assert_method_defined("name", ia.LdaModel)
        self.assert_method_defined("train", ia.LdaModel)

    def assert_method_defined(self, method, clazz):
        self.assertTrue(method in dir(clazz), "method " + method + " didn't exist on " + str(clazz))


if __name__ == "__main__":
    unittest.main()
