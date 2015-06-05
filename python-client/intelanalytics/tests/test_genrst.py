import iatest
iatest.init()
import unittest

from intelanalytics.meta.command import Doc

class DocExtraction(unittest.TestCase):

    doc1 = """Computes a cumulative percent sum.

    A cumulative percent sum is computed by sequentially stepping through the
    column values and keeping track of the current percentage of the total sum
    accounted for at the current value."""

    def test_get_from_str(self):
        d = Doc.get_from_str(self.doc1)
        self.assertEqual("Computes a cumulative percent sum.", d.one_line)
        self.assertEqual("""A cumulative percent sum is computed by sequentially stepping through the
column values and keeping track of the current percentage of the total sum
accounted for at the current value.""", d.extended)

if __name__ == '__main__':
    unittest.main()
