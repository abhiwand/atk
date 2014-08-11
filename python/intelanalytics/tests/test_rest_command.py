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

    def test_extract_columns_from_data(self):
        executor = Executor()
        indices = [0, 2]
        data = [[1, 'a', '3'], [2, 'b', '2'], [3, 'c', '5'], [4, 'd', '-10']]
        result = executor.extract_data_from_selected_columns(data, indices)
        self.assertEqual(result, [[1, '3'], [2, '2'], [3, '5'], [4, '-10']])

if __name__ == '__main__':
    unittest.main()
