import unittest
import os
import sys
from mock import Mock
_current_dir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(
    os.path.join(os.path.join(_current_dir, os.pardir), os.pardir)))

from intel_analytics.table.pig.pig_kfold_split import generate_split_statement

features = ['test1', 'test2', 'test3']
feature_list = ", ".join([f for f in features])
class TestTransform(unittest.TestCase):
    def test_kfold(self):
        cmd_line_args = Mock()
        cmd_line_args.test_fold_id = '2'
        cmd_line_args.input_column = 'input'
        cmd_line_args.split_name = ['TE', 'TR']
        cmd_line_args.new_feature_name = 'new_f'
        statements = generate_split_statement(feature_list, cmd_line_args)
        self.assertEqual(statements, "test1, test2, test3, (( 2 <= input and input < 3 )? 'TE'  :  'TR') ")

if __name__ == '__main__':
    unittest.main()
