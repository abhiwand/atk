##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
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
"""
Default, backend stub, does little beyond logging
"""
import logging
logger = logging.getLogger(__name__)
from column import BigColumn
from files import CsvFile


class FrameBackendSimplePrint(object):
    """Simple backend stub that just prints expected backend activity"""

    def append(self, frame, data):
        logger.info("Backend: Appending data to frame {0}: {1}".format(repr(frame), repr(data)))
        # hack back end to create columns
        if isinstance(data, list):
            for d in data:
                self.append(frame, d)
            return

        if isinstance(data, CsvFile):
            for name, data_type in data.fields:
                frame._columns[name] = BigColumn(name, data_type)
        elif isinstance(data, BigColumn):
            frame._columns[data.name] = BigColumn(data.name, data.data_type)
        else:
            raise TypeError("Unsupported append data type "
                            + data.__class__.__name__)

    def assign(self, dst, value):
        logger.info("Backend: Assignment {0} = {1}".format(repr(dst), repr(value)))
        if hasattr(dst, 'frame'):
            if dst not in dst.frame._columns:
                dst.frame._columns[dst.name] = dst
        else:
            logger.info("(Doing Nothing for Assignment)")


    def copy_columns(self, frame, dst_list, src_list):
        logger.info("Backend: copy_columns([{0}], [{1}])".format(','.join([repr(dst) for dst in dst_list]),
                                                                 ','.join([repr(src) for src in src_list])))
        for i, key in enumerate(dst_list):
            frame._columns[key] = src_list[i]

    def delete_column(self, frame, name):
        logger.info("Backend: Dropping columns {0} from frame {1}".format(name, repr(frame)))

    def drop_rows(self, frame, predicate):
        logger.info("Backend: Dropping rows from frame {0} where {1}".format(repr(frame), repr(predicate)))

    def rename_columns(self, frame, name_pairs):
        old, new = zip(*name_pairs)
        logger.info("Backend: Renaming columns in frame {0} from {1} to {2}".format(repr(frame), ','.join(old), ','.join(new)))

    def save(self, frame, name):
        logger.info("Backend: Saving frame {0} to '{1}'".format(repr(frame), name))
