import iatest
iatest.init()

import unittest
from mock import patch, Mock
from intelanalytics.rest.command import print_progress


class TestRestCommand(unittest.TestCase):

    @patch('intelanalytics.rest.command.sys.stdout')
    def test_print_initialization(self, stdout):
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        print_progress([], False)
        self.assertEqual(len(write_queue), 1)
        self.assertEqual(write_queue[0], "\rinitializing...")

    @patch('intelanalytics.rest.command.sys.stdout')
    def test_print_receive_progress(self, stdout):
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        print_progress([], False)


        print_progress([30], False)

        self.assertEqual(len(write_queue), 2)
        self.assertEqual(write_queue[0], "\rinitializing...")
        self.assertEqual(write_queue[1], "\r 30% [===============...................................]")


if __name__ == '__main__':
    unittest.main()
