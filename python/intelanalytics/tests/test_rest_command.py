//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

import iatest
iatest.init()

import unittest
from mock import patch, Mock
from intelanalytics.rest.command import ProgressPrinter
from intelanalytics.rest.command import Executor


class TestRestCommand(unittest.TestCase):

    @patch('intelanalytics.rest.command.sys.stdout')
    def test_print_initialization(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], False)
        self.assertEqual(len(write_queue), 1)
        self.assertEqual(write_queue[0], "\rinitializing...")

    @patch('intelanalytics.rest.command.sys.stdout')
    def test_print_receive_progress(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], False)
        printer.print_progress([{"progress": 30.0, "tasks_info": {"retries": 0}}], False)

        self.assertEqual(len(write_queue), 2)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r[=======..................]  30.00% Tasks retries:0 Time 0:00:00")

    @patch('intelanalytics.rest.command.sys.stdout')
    def test_print_next_progress(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], False)
        printer.print_progress([{"progress": 20.0, "tasks_info": {"retries": 0}}], False)
        self.assertEqual(len(write_queue), 2)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r[=====....................]  20.00% Tasks retries:0 Time 0:00:00")

        printer.print_progress([{"progress": 50.0, "tasks_info": {"retries": 0}},
                                {"progress": 100.0, "tasks_info": {"retries": 0}}], True)

        self.assertEqual(len(write_queue), 4)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r[=====....................]  20.00% Tasks retries:0 Time 0:00:00")
        self.assertEqual(write_queue[2], "\r[============.............]  50.00% Tasks retries:0 Time 0:00:00\n")
        self.assertEqual(write_queue[3], "\r[=========================] 100.00% Tasks retries:0 Time 0:00:00\n")

    @patch('intelanalytics.rest.command.sys.stdout')
    def test_multiple_progress_come_right_after_initializing_stage(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], False)
        printer.print_progress([{"progress": 100.0, "tasks_info": {"retries": 0}},
                                {"progress": 100.0, "tasks_info": {"retries": 0}},
                                {"progress": 50.0, "tasks_info": {"retries": 0}}], True)

        self.assertEqual(len(write_queue), 4)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r[=========================] 100.00% Tasks retries:0 Time 0:00:00\n")
        self.assertEqual(write_queue[2], "\r[=========================] 100.00% Tasks retries:0 Time 0:00:00\n")
        self.assertEqual(write_queue[3], "\r[============.............]  50.00% Tasks retries:0 Time 0:00:00\n")

    @patch('intelanalytics.rest.command.sys.stdout')
    def test_multiple_progress_come_as_finished(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], False)
        printer.print_progress([{"progress": 30.0, "tasks_info": {"retries": 0}}], False)

        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r[=======..................]  30.00% Tasks retries:0 Time 0:00:00")
        printer.print_progress([{"progress": 100.0, "tasks_info": {"retries": 0}},
                                {"progress": 100.0, "tasks_info": {"retries": 0}},
                                {"progress": 100.0, "tasks_info": {"retries": 0}}], True)

        self.assertEqual(len(write_queue), 5)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r[=======..................]  30.00% Tasks retries:0 Time 0:00:00")
        self.assertEqual(write_queue[2], "\r[=========================] 100.00% Tasks retries:0 Time 0:00:00\n")
        self.assertEqual(write_queue[3], "\r[=========================] 100.00% Tasks retries:0 Time 0:00:00\n")
        self.assertEqual(write_queue[4], "\r[=========================] 100.00% Tasks retries:0 Time 0:00:00\n")

if __name__ == '__main__':
    unittest.main()
