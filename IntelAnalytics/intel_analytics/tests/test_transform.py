import unittest
from mock import Mock
from intel_analytics.table.builtin_functions import EvalFunctions
from intel_analytics.table.pig.pig_transform import generate_transform_statement

features = ['f1', 'f2', 'f3']
class TestTransform(unittest.TestCase):
    def test_ABS(self):
        cmd_line_args = Mock()
        cmd_line_args.keep_original_feature = True
        cmd_line_args.is_standardization = False
        cmd_line_args.transformation_function_args = None
        cmd_line_args.new_feature_name = 'new_f'
        cmd_line_args.feature_to_transform = 'f1'
        cmd_line_args.transformation_function = EvalFunctions.to_string(EvalFunctions.Math.ABS)
        statements = generate_transform_statement(features, cmd_line_args)
        self.assertEqual(statements, 'f1, f2, f3, ABS(f1) as new_f')

    def test_EXP(self):
        cmd_line_args = Mock()
        cmd_line_args.keep_original_feature = True
        cmd_line_args.is_standardization = False
        cmd_line_args.transformation_function_args = None
        cmd_line_args.new_feature_name = 'new_f'
        cmd_line_args.feature_to_transform = 'f1'
        cmd_line_args.transformation_function = EvalFunctions.to_string(EvalFunctions.Math.EXP)
        statements = generate_transform_statement(features, cmd_line_args)
        self.assertEqual(statements, 'f1, f2, f3, EXP(f1) as new_f')

    def test_FLOOR(self):
        cmd_line_args = Mock()
        cmd_line_args.keep_original_feature = True
        cmd_line_args.is_standardization = False
        cmd_line_args.transformation_function_args = None
        cmd_line_args.new_feature_name = 'new_f'
        cmd_line_args.feature_to_transform = 'f1'
        cmd_line_args.transformation_function = EvalFunctions.to_string(EvalFunctions.Math.FLOOR)
        statements = generate_transform_statement(features, cmd_line_args)
        self.assertEqual(statements, 'f1, f2, f3, FLOOR(f1) as new_f')

    def test_CEIL(self):
        cmd_line_args = Mock()
        cmd_line_args.keep_original_feature = True
        cmd_line_args.is_standardization = False
        cmd_line_args.transformation_function_args = None
        cmd_line_args.new_feature_name = 'new_f'
        cmd_line_args.feature_to_transform = 'f1'
        cmd_line_args.transformation_function = EvalFunctions.to_string(EvalFunctions.Math.CEIL)
        statements = generate_transform_statement(features, cmd_line_args)
        self.assertEqual(statements, 'f1, f2, f3, CEIL(f1) as new_f')


    def test_LOG(self):
        cmd_line_args = Mock()
        cmd_line_args.keep_original_feature = True
        cmd_line_args.is_standardization = False
        cmd_line_args.transformation_function_args = None
        cmd_line_args.new_feature_name = 'new_f'
        cmd_line_args.feature_to_transform = 'f1'
        cmd_line_args.transformation_function = EvalFunctions.to_string(EvalFunctions.Math.LOG)
        statements = generate_transform_statement(features, cmd_line_args)
        self.assertEqual(statements, 'f1, f2, f3, LOG(f1) as new_f')

    def test_LOG10(self):
        cmd_line_args = Mock()
        cmd_line_args.keep_original_feature = True
        cmd_line_args.is_standardization = False
        cmd_line_args.transformation_function_args = None
        cmd_line_args.new_feature_name = 'new_f'
        cmd_line_args.feature_to_transform = 'f1'
        cmd_line_args.transformation_function = EvalFunctions.to_string(EvalFunctions.Math.LOG10)
        statements = generate_transform_statement(features, cmd_line_args)
        self.assertEqual(statements, 'f1, f2, f3, LOG10(f1) as new_f')

    def test_ROUND(self):
        cmd_line_args = Mock()
        cmd_line_args.keep_original_feature = True
        cmd_line_args.is_standardization = False
        cmd_line_args.transformation_function_args = None
        cmd_line_args.new_feature_name = 'new_f'
        cmd_line_args.feature_to_transform = 'f1'
        cmd_line_args.transformation_function = EvalFunctions.to_string(EvalFunctions.Math.ROUND)
        statements = generate_transform_statement(features, cmd_line_args)
        self.assertEqual(statements, 'f1, f2, f3, ROUND(f1) as new_f')

    def test_SQRT(self):
        cmd_line_args = Mock()
        cmd_line_args.keep_original_feature = True
        cmd_line_args.is_standardization = False
        cmd_line_args.transformation_function_args = None
        cmd_line_args.new_feature_name = 'new_f'
        cmd_line_args.feature_to_transform = 'f1'
        cmd_line_args.transformation_function = EvalFunctions.to_string(EvalFunctions.Math.SQRT)
        statements = generate_transform_statement(features, cmd_line_args)
        self.assertEqual(statements, 'f1, f2, f3, SQRT(f1) as new_f')

    def test_DIV_by_constant(self):
        cmd_line_args = Mock()
        cmd_line_args.keep_original_feature = True
        cmd_line_args.is_standardization = False
        cmd_line_args.transformation_function_args = [10]
        cmd_line_args.new_feature_name = 'new_f'
        cmd_line_args.feature_to_transform = 'f1'
        cmd_line_args.transformation_function = EvalFunctions.to_string(EvalFunctions.Math.DIV)
        statements = generate_transform_statement(features, cmd_line_args)
        self.assertEqual(statements, 'f1, f2, f3, (f1 / 10) as new_f')

    def test_DIV_by_column(self):
        cmd_line_args = Mock()
        cmd_line_args.keep_original_feature = True
        cmd_line_args.is_standardization = False
        cmd_line_args.transformation_function_args = ['f2']
        cmd_line_args.new_feature_name = 'new_f'
        cmd_line_args.feature_to_transform = 'f1'
        cmd_line_args.transformation_function = EvalFunctions.to_string(EvalFunctions.Math.DIV)
        statements = generate_transform_statement(features, cmd_line_args)
        self.assertEqual(statements, 'f1, f2, f3, (f1 / f2) as new_f')

    def test_MOD_by_constant(self):
        cmd_line_args = Mock()
        cmd_line_args.keep_original_feature = True
        cmd_line_args.is_standardization = False
        cmd_line_args.transformation_function_args = [10]
        cmd_line_args.new_feature_name = 'new_f'
        cmd_line_args.feature_to_transform = 'f1'
        cmd_line_args.transformation_function = EvalFunctions.to_string(EvalFunctions.Math.MOD)
        statements = generate_transform_statement(features, cmd_line_args)
        self.assertEqual(statements, 'f1, f2, f3, (f1 % (int) 10) as new_f')

    def test_MOD_by_column(self):
        cmd_line_args = Mock()
        cmd_line_args.keep_original_feature = True
        cmd_line_args.is_standardization = False
        cmd_line_args.transformation_function_args = ['f2']
        cmd_line_args.new_feature_name = 'new_f'
        cmd_line_args.feature_to_transform = 'f1'
        cmd_line_args.transformation_function = EvalFunctions.to_string(EvalFunctions.Math.MOD)
        statements = generate_transform_statement(features, cmd_line_args)
        self.assertEqual(statements, 'f1, f2, f3, (f1 % (int) f2) as new_f')


    def test_RANDOM(self):
        cmd_line_args = Mock()
        cmd_line_args.keep_original_feature = True
        cmd_line_args.is_standardization = False
        cmd_line_args.transformation_function_args = [100, 200]
        cmd_line_args.new_feature_name = 'new_f'
        cmd_line_args.feature_to_transform = 'f1'
        cmd_line_args.transformation_function = EvalFunctions.to_string(EvalFunctions.Math.RANDOM)
        statements = generate_transform_statement(features, cmd_line_args)
        self.assertEqual(statements, 'f1, f2, f3, (100 + RANDOM() * 100) as new_f')



if __name__ == '__main__':
    unittest.main()
