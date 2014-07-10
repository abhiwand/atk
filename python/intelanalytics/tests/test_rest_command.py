import iatest
iatest.init()

import unittest
from mock import patch, Mock
from intelanalytics.rest.command import ProgressPrinter


class TestRestCommand(unittest.TestCase):

    @patch('intelanalytics.rest.command.sys.stdout')
    def test_print_initialization(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], [], False)
        self.assertEqual(len(write_queue), 1)
        self.assertEqual(write_queue[0], "\rinitializing...")

    @patch('intelanalytics.rest.command.sys.stdout')
    def test_print_receive_progress(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], [], False)
        printer.print_progress([30], ["Tasks Succeeded: 93 Failed: 0"], False)

        self.assertEqual(len(write_queue), 2)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r 30.00% [===============...................................] Tasks Succeeded: 93 Failed: 0 [Elapsed Time 0:00:00]")

    @patch('intelanalytics.rest.command.sys.stdout')
    def test_print_next_progress(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], [], False)
        printer.print_progress([20], ["Tasks Succeeded: 4 Failed: 0"], False)
        printer.print_progress([100, 50], ["Tasks Succeeded: 20 Failed: 0", "Tasks Succeeded: 4 Failed: 0"], False)

        self.assertEqual(len(write_queue), 4)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r 20.00% [==========........................................] Tasks Succeeded: 4 Failed: 0 [Elapsed Time 0:00:00]")
        self.assertEqual(write_queue[2], "\r100.00% [==================================================] Tasks Succeeded: 20 Failed: 0 [Elapsed Time 0:00:00]\n")
        self.assertEqual(write_queue[3], "\r 50.00% [=========================.........................] Tasks Succeeded: 4 Failed: 0 [Elapsed Time 0:00:00]")

    @patch('intelanalytics.rest.command.sys.stdout')
    def test_multiple_progress_come_right_after_initializing_stage(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], [], False)
        printer.print_progress([100, 100, 50], ["Tasks Succeeded: 10 Failed: 0", "Tasks Succeeded: 10 Failed: 0", "Tasks Succeeded: 5 Failed: 0"], True)
        self.assertEqual(len(write_queue), 4)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r100.00% [==================================================] Tasks Succeeded: 10 Failed: 0 [Elapsed Time 0:00:00]\n")
        self.assertEqual(write_queue[2], "\r100.00% [==================================================] Tasks Succeeded: 10 Failed: 0 [Elapsed Time 0:00:00]\n")
        self.assertEqual(write_queue[3], "\r 50.00% [=========================.........................] Tasks Succeeded: 5 Failed: 0 [Elapsed Time 0:00:00]\n")

    @patch('intelanalytics.rest.command.sys.stdout')
    def test_multiple_progress_come_as_finished(self, stdout):
        printer = ProgressPrinter()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        printer.print_progress([], [], False)
        printer.print_progress([30], ["Tasks Succeeded: 93 Failed: 0"], False)

        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r 30.00% [===============...................................] Tasks Succeeded: 93 Failed: 0 [Elapsed Time 0:00:00]")
        printer.print_progress([100, 100, 100], ["Tasks Succeeded: 186 Failed: 0", "Tasks Succeeded: 93 Failed: 0", "Tasks Succeeded: 93 Failed: 0"], True)

        self.assertEqual(len(write_queue), 5)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r 30.00% [===============...................................] Tasks Succeeded: 93 Failed: 0 [Elapsed Time 0:00:00]")
        self.assertEqual(write_queue[2], "\r100.00% [==================================================] Tasks Succeeded: 186 Failed: 0 [Elapsed Time 0:00:00]\n")
        self.assertEqual(write_queue[3], "\r100.00% [==================================================] Tasks Succeeded: 93 Failed: 0 [Elapsed Time 0:00:00]\n")
        self.assertEqual(write_queue[4], "\r100.00% [==================================================] Tasks Succeeded: 93 Failed: 0 [Elapsed Time 0:00:00]\n")


if __name__ == '__main__':
    unittest.main()
