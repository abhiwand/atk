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
        self.assert_methods_defined(['annotation',
                                    'field_names',
                                    'field_types'], ia.CsvFile)

    def test_expected_methods_exist_on_frame(self):
        self.assert_methods_defined(['add_columns',
                                      'append',
                                      'assign_sample',
                                      'bin_column',
                                      'bin_column_equal_depth',
                                      'bin_column_equal_width',
                                      'classification_metrics',
                                      'column_median',
                                      'column_mode',
                                      'column_names',
                                      'column_summary_statistics',
                                      'copy',
                                      'correlation',
                                      'correlation_matrix',
                                      'count',
                                      'covariance',
                                      'covariance_matrix',
                                      'cumulative_percent',
                                      'cumulative_sum',
                                      'dot_product',
                                      'download',
                                      'drop_columns',
                                      'drop_duplicates',
                                      'drop_rows',
                                      'ecdf',
                                      'entropy',
                                      'export_to_csv',
                                      'export_to_json',
                                      'filter',
                                      'flatten_column',
                                      'get_error_frame',
                                      'group_by',
                                      'histogram',
                                      'inspect',
                                      'join',
                                      'name',
                                      'quantiles',
                                      'rename_columns',
                                      'row_count',
                                      'schema',
                                      'sort',
                                      'status',
                                      'take',
                                      'tally',
                                      'tally_percent',
                                      'top_k'], ia.Frame)

    def test_expected_methods_exist_on_graph(self):
        self.assert_methods_defined(['annotate_degrees',
                                     'annotate_weighted_degrees',
                                     'clustering_coefficient',
                                     'copy',
                                     'define_edge_type',
                                     'define_vertex_type',
                                     'edge_count',
                                     'edges',
                                     'export_to_titan',
                                     'graphx_connected_components',
                                     'graphx_pagerank',
                                     'graphx_triangle_count',
                                     'ml',
                                     'name',
                                     'status',
                                     'vertex_count',
                                     'vertices'], ia.Graph)

    def test_expected_methods_exist_on_titangraph(self):
        self.assert_methods_defined(['annotate_degrees',
                                     'annotate_weighted_degrees',
                                     'append',
                                     'clustering_coefficient',
                                     'connected_components',
                                     'copy',
                                     'export_to_graph',
                                     'graphx_connected_components',
                                     'graphx_pagerank',
                                     'graphx_triangle_count',
                                     'hierarchical_clustering',
                                     'load',
                                     'ml',
                                     'name',
                                     'page_rank',
                                     'query',
                                     'sampling',
                                     'status'], ia.TitanGraph)

    def test_expected_methods_exist_on_kmeansmodel(self):
        self.assert_methods_defined(["name",
                                     "predict",
                                     "train"], ia.KMeansModel)

    def test_expected_methods_exist_on_ldamodel(self):
        self.assert_methods_defined(["name",
                                    "train"], ia.LdaModel)

    def assert_methods_defined(self, methods, clazz):
        for method in methods:
            self.assertTrue(method in dir(clazz), "method " + method + " didn't exist on " + str(clazz))


if __name__ == "__main__":
    unittest.main()
