import unittest
import os
import sys
from mock import Mock
_current_dir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(
    os.path.join(os.path.join(_current_dir, os.pardir), os.pardir)))

from intel_analytics.table.pig.pig_autosplit import generate_split_statement

features = ['test1', 'test2', 'test3']
class TestTransform(unittest.TestCase):
    def test_kfold(self):
        cmd_line_args = Mock()
        cmd_line_args.test_fold_id = '2'
        cmd_line_args.input_column = 'input'
        cmd_line_args.split_name = ['TE', 'TR']
        cmd_line_args.new_feature_name = 'new_f'
        statements = generate_split_statement(features, cmd_line_args)
        self.assertEqual(statements, " test1, test2, test3, (( 2 <= input and input < 3 )? 'TE' : 'TR') ")

    def test_percent(self):
        cmd_line_args = Mock()
        cmd_line_args.test_fold_id = '0'
        cmd_line_args.input_column = 'input'
        cmd_line_args.split_percent = [50,30,20]
        cmd_line_args.split_name = ['A', 'B', 'C']
        cmd_line_args.new_feature_name = 'new_f'
        statements = generate_split_statement(features, cmd_line_args)
        self.assertEqual(statements, " test1, test2, test3,  (CASE WHEN  (input < 50)  THEN 'A'  WHEN  (50 <= input and input < 80)  THEN 'B'  ELSE 'C'  END) ")


if __name__ == '__main__':
    unittest.main()
