##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################

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
