import unittest
from mock import Mock
from intel_analytics.table.pig import pig_column_stats

features = ['f1', 'f2', 'f3']
class TestColumnStatistics(unittest.TestCase):
    def test_add_quote(self):
        val = 'lorem ipsum'
        result = add_quotes(val)
        self.assertEqual(result, '\'lorem ipsum\'')

    def test_add_quote_complex(self):
        val = 'lorem \'\"ipsum'
        result = add_quotes(val)
        self.assertEqual(result, '\'lorem \'\"ipsum\'')

    def test_escape_single_quotes(self):
        val = 'abcd\'efgh'
        result = escape_single_quotes(val)
        self.assertEqual(result,'abcd\\\'efgh')

    def test_generate_interval_check_num_exclusive(self):
        interval = Mock()
        interval.lower_bound = 4
        interval.upper_bound = 5
        interval.lower_closed = True
        interval.upper_closed = True
        result = generate_interval_check(features[0], interval)
        self.assertEqual(result, '4 <= f1 AND f1 <= 5')

    def test_generate_interval_check_num_inclusive(self):
        interval = Mock()
        interval.lower_bound = 4
        interval.lower_closed = False
        interval.upper_bound = 5
        interval.upper_closed = False
        result = generate_interval_check(features[1], interval)
        self.assertEqual(result, '4 < f1 AND f1 < 5')

    def test_replace_inf(self):
        str = 'Interval(-Inf, Inf, lower_closed=True, upper_closed=False)'
        expected_result = 'Interval(Smallest(), Largest(), lower_closed=True, upper_closed=False)'
        result = replace_inf(str)
        self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main()
