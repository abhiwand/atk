#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import intelanalytics as ia

# show full stack traces
ia.errors.show_details = True
ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()

class FrameDropTest(unittest.TestCase):
    """
    Test frame drop()

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_frame_drop(self):
        print "define csv file"
        csv = ia.CsvFile("/datasets/classification-compute.csv", schema= [('a', str),
                                                                          ('b', ia.int32),
                                                                          ('labels', ia.int32),
                                                                          ('predictions', ia.int32)], delimiter=',', skip_header_lines=1)

        print "create frame"
        frame = ia.Frame(csv, name="test_frame_drop")

        print "dropping frame by entity"
        ia.drop_frames(frame)
        frames = ia.get_frame_names()
        self.assertFalse("test_frame_drop" in frames, "test_frame_drop should not exist in list of frames")

        frame = ia.Frame(csv, name="test_frame_drop")

        print "dropping frame by name"
        ia.drop_frames("test_frame_drop")
        self.assertFalse("test_frame_drop" in frames, "test_frame_drop should not exist in list of frames")


if __name__ == "__main__":
    unittest.main()
