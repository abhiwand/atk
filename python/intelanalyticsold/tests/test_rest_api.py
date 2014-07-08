__author__ = 'mohitdee'
import iatest
iatest.init()
import unittest
from mock import patch
from intelanalytics.core.backend import FrameBackendSimplePrint
from intelanalytics.core.frame import BigFrame
from intelanalytics.core.column import BigColumn
from intelanalytics.core.files import CsvFile
from intelanalytics.core.types import *

def get_simple_frame_abcde():
    return BigFrame(CsvFile("dummy.csv", [('A', str),
                                          ('B', str),
                                          ('C', str),
                                          ('D', float64),
                                          ('E', str)]))
@patch('intelanalytics.core.config.get_frame_backend', new=FrameBackendSimplePrint)
class FrameConstruction(unittest.TestCase):
     def test_create_from_csv(self):
        csvfile = CsvFile("dummy.csv", [('A', int32), ('B', int64)])
        f = BigFrame()
        f.append(csvfile)
        self.assertEqual(2, len(f))
        self.assertTrue(isinstance(f['A'], BigColumn))
        self.assertTrue(isinstance(f['B'], BigColumn))
        try:
            c = f['C']
            self.fail()
        except KeyError:
            pass
if __name__ == '__main__':
    unittest.main()