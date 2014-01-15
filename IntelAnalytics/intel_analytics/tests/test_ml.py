##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
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
Unit tests for Class TitanGiraphMachineLearning in intel_analytics/graph/titan/ml.py
"""

import unittest
import os
import sys
from mock import patch, Mock, MagicMock, sentinel

_current_dir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(
    os.path.join(os.path.join(_current_dir, os.pardir), os.pardir)))

if __name__ == '__main__':
    sys.modules['intel_analytics.config'] = __import__('mock_config')
    sys.modules['intel_analytics.subproc'] = __import__('mock_subproc')
    #sys.modules['intel_analytics.report'] = __import__('mock_report')
    sys.modules['intel_analytics.progress'] = __import__('mock_progress')
else:
    #to get coverage on all of our modules we need to execute the unit tests utilizing a test runner
    #this runner executes all of the test files in the same execution space making it so that import from previous
    #files are still in sys.modules we need to do the following to reset the required modules so that imports work as
    #expected
    import intel_analytics.tests.mock_config, intel_analytics.tests.mock_subproc, intel_analytics.tests.mock_progress

    print intel_analytics.tests.mock_config, intel_analytics.tests.mock_subproc, intel_analytics.tests.mock_progress

    mocked_modules = ['intel_analytics.config', 'intel_analytics.subproc', 'intel_analytics.progress']

    old_modules = {}
    for module in mocked_modules:
        if module in sys.modules:
            old_modules[module] = sys.modules[module]
        else:
            old_modules[module] = None


    sys.modules['intel_analytics.config'] = sys.modules['intel_analytics.tests.mock_config']
    sys.modules['intel_analytics.subproc'] = sys.modules['intel_analytics.tests.mock_subproc']
    sys.modules['intel_analytics.progress'] = sys.modules['intel_analytics.tests.mock_progress']

from intel_analytics.graph.titan.ml import TitanGiraphMachineLearning


class TestsTitanGiraphMachineLearning(unittest.TestCase):
    def setUp(self):
        self.graph = Mock()
        self.graph.titan_table_name = 'test_table'
        self.graph.user_graph_name = 'test_graph'

    def test_start(self):
        ml = TitanGiraphMachineLearning(self.graph)
        self.assertEqual(self.graph, ml._graph)
        self.assertEqual('test_table', ml._table_name)

    @classmethod
    def tearDownClass(cls):
        '''This method will revert the mocked modules so that the test runner can continue executing tests that do not mock modules'''
        if __name__ != '__main__':
            for module in mocked_modules:
                if old_modules[module] == None:
                    del sys.modules[module]
                else:
                    sys.modules[module] = old_modules[module]
        else:
            pass

    @patch('__builtin__.open')
    def test_page_rank_required_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.page_rank('test_edge_property',
                              'test_edge_label',
                              'test_output_vertex_properties')
        self.assertEqual('test_graph', result.graph_name)

    @patch('__builtin__.open')
    def test_page_rank_optional_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.page_rank('test_edge_property',
                              'test_edge_label',
                              'test_output_vertex_properties',
                              num_worker='15')
        self.assertEqual('test_graph', result.graph_name)

    @patch('__builtin__.open')
    def test_belief_prop_required_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.belief_prop('test_vertex_properties',
                                'test_edge_property',
                                'test_edge_label',
                                'test_output_vertex_properties')
        self.assertEqual('test_graph', result.graph_name)

    @patch('__builtin__.open')
    def test_belief_prop_optional_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.belief_prop('test_vertex_properties',
                                'test_edge_property',
                                'test_edge_label',
                                'test_output_vertex_properties',
                                max_supersteps='25')
        self.assertEqual('test_graph', result.graph_name)

    @patch('__builtin__.open')
    def test_apl_required_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.avg_path_len('test_edge_label',
                                 'test_output_vertex_properties')
        self.assertEqual('test_graph', result.graph_name)

    @patch('__builtin__.open')
    def test_apl_optional_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.avg_path_len('test_edge_label',
                                 'test_output_vertex_properties',
                                 num_worker='3')
        self.assertEqual('test_graph', result.graph_name)

    @patch('__builtin__.open')
    def test_label_prop_required_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.label_prop('test_vertex_properties',
                               'test_edge_property',
                               'test_edge_label',
                               'test_output_vertex_properties')
        self.assertEqual('test_graph', result.graph_name)

    @patch('__builtin__.open')
    def test_label_prop_optional_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.label_prop('test_vertex_properties',
                               'test_edge_property',
                               'test_edge_label',
                               'test_output_vertex_properties',
                               num_worker='7')
        self.assertEqual('test_graph', result.graph_name)

    @patch('__builtin__.open')
    def test_lda_required_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.lda('test_edge_property',
                        'test_edge_label',
                        'test_output_vertex_properties',
                        'test_vertex_type',
                        'test_edge_type')
        self.assertEqual('test_graph', result.graph_name)

    @patch('__builtin__.open')
    def test_lda_optional_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.lda('test_edge_property',
                        'test_edge_label',
                        'test_output_vertex_properties',
                        'test_vertex_type',
                        'test_edge_type',
                        max_supersteps='30')
        self.assertEqual('test_graph', result.graph_name)

    @patch('__builtin__.open')
    def test_als_required_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.als('test_edge_property',
                        'test_edge_label',
                        'test_output_vertex_properties',
                        'test_vertex_type',
                        'test_edge_type')
        self.assertEqual('test_graph', result.graph_name)

    @patch('__builtin__.open')
    def test_als_optional_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.als('test_edge_property',
                        'test_edge_label',
                        'test_output_vertex_properties',
                        'vertex_type',
                        'test_edge_type',
                        max_supersteps='10')
        self.assertEqual('test_graph', result.graph_name)

    @patch('__builtin__.open')
    def test_cgd_required_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.cgd('test_edge_property',
                        'test_edge_label',
                        'test_output_vertex_properties',
                        'test_vertex_type',
                        'test_edge_type')
        self.assertEqual('test_graph', result.graph_name)

    @patch('__builtin__.open')
    def test_cgd_optional_inputs(self, mock_open):
        ml = TitanGiraphMachineLearning(self.graph)
        result = ml.cgd('test_edge_property',
                        'test_edge_label',
                        'test_output_vertex_properties',
                        'vertex_type',
                        'test_edge_type',
                        max_supersteps='10')
        self.assertEqual('test_graph', result.graph_name)

    def test_recommend_throw_exception(self):
        ml = TitanGiraphMachineLearning(self.graph)
        #expect to have ValueError
        with self.assertRaises(ValueError):
            ml.recommend('101010')

    def test_recommend_normal(self):
        ml = TitanGiraphMachineLearning(self.graph)
        ml._output_vertex_property_list = 'test_vertex_properties'
        ml._vertex_type = 'test_vertex_type'
        ml._edge_type = 'test_edge_type'
        result = ml.recommend('101010')
        self.assertEqual('test_graph', result.graph_name)
        self.assertEqual([], result.recommend_id)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
