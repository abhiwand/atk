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
Unit tests for intel_analytics/graph/titan/ml.py  GroovyProgressReportStrategy
"""
import unittest
import os
import sys

#make sure SUT python module can be found
_current_dir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(
    os.path.join(os.path.join(_current_dir, os.pardir), os.pardir)))

from intel_analytics.graph.titan.ml import GroovyProgressReportStrategy


class TestGroovyProgressReportStrategy(unittest.TestCase):
    def test_start(self):
        strategy = GroovyProgressReportStrategy()
        self.assertEquals("Progress", strategy.progress_bar.name)
        self.assertEquals(100, strategy.progress_bar.value)
        self.assertTrue(strategy.progress_bar.is_in_animation)

    def test_complete_message(self):
        strategy = GroovyProgressReportStrategy()
        strategy.report("Top 10 recommendations")
        self.assertTrue(strategy.progress_bar.is_in_animation)
        strategy.report("complete execution")
        self.assertFalse(strategy.progress_bar.is_in_animation)

    def test_handle_error(self):
        strategy = GroovyProgressReportStrategy()
        strategy.handle_error(1, "javax.script.ScriptException: com.tinkerpop.pipes.util.FastNoSuchElementException")
        self.assertTrue(strategy.progress_bar.is_in_alert)


    def test_title_update_when_complete(self):
        strategy = GroovyProgressReportStrategy()
        strategy.report("complete execution")
        self.assertEquals("Execution completed", strategy.progress_bar.name)

    def test_title_update_when_fail(self):
        strategy = GroovyProgressReportStrategy()
        strategy.handle_error(1, "javax.script.ScriptException: com.tinkerpop.pipes.util.FastNoSuchElementException")
        self.assertEquals("Execution failed", strategy.progress_bar.name)

if __name__ == '__main__':
    unittest.main()
