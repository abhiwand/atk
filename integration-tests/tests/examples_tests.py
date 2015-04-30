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
#ia.loggers.set_api()
# TODO: port setup should move to a super class
if ia.server.port != 19099:
    ia.server.port = 19099
ia.connect()




#import intelanalytics as ia
#ia.server.port = 19099
#ia.connect()


import doctest
doctest.ELLIPSIS_MARKER = "-etc-"

import sys
current_module = sys.modules[__name__]

import os
#print "examples=%s" % examples
here = os.path.dirname(os.path.abspath(__file__))
path_to_examples = os.path.join(here, "../../python/intelanalytics/doc/examples")
import fnmatch

__test__ = {}


# option 5
def get_all_example_rst_file_paths():
    paths = []
    for root, dirnames, filenames in os.walk(path_to_examples):
        for filename in fnmatch.filter(filenames, '*.rst'):
            paths.append(os.path.join(root, filename))
    return paths


def add_rst_file(full_path):
    with open(full_path) as f:
        content = f.read()
    __test__[full_path] = content


def init_tests(files):
    if isinstance(files, basestring):
        files = [files]
    __test__.clear()
    for f in files:
        add_rst_file(f)


def run_tests(files=None, verbose=False):
    init_tests(files or get_all_example_rst_file_paths())
    return doctest.testmod(m=current_module,
                    raise_on_error=True,
                    exclude_empty=True,
                    verbose=verbose,
                    optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)


class ExampleDocTests(unittest.TestCase):

    def test_examples(self):
        print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"

        results = run_tests()
        self.assertEqual(0, results.failed, "Tests in the example documentation failed.")
        print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"


if __name__ == "__main__":
    unittest.main()


#run_tests(verbose=True)

# option 4
# def run_example(relative_path, verbose=False):
#     here = os.path.dirname(os.path.abspath(__file__))
#     path_to_examples = os.path.join(here, "../../python/intelanalytics/doc/examples")
#
#     __test__['bin_column'] = content
#     print "Running examples in %s" % relative_path
#     return doctest.testmod(exclude_empty=True,
#                            verbose=verbose,
#                            optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
#
# bin_column_path = "frame/bin_column.rst"
#
# result = run_example(bin_column_path)
#
# print "result=%s" % str(result)
# print "failed=%s" % result.failed


# option 3
# def run_example(relative_path):
#     with open(os.path.join(path_to_examples, "frame/bin_column.rst")) as f:
#         content = f.read()
#
#     print "Running examples in %s" % relative_path
#     return doctest.run_docstring_examples(content,
#                                           {"ia": ia},
#                                           verbose=True,
#                                           optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
#
# bin_column_path = "frame/bin_column.rst"
#
# result = run_example(bin_column_path)
#
# print "result=%s" % result


# option 2
#connection_header = """
#>>> import intelanalytics as ia
#>>> ia.server.port = 19099
#>>> ia.connect()
#-etc-
#"""
#content = "\n".join([connection_header, content])
#doctest.run_docstring_examples(content, {}, verbose=True, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)

# option 1
#doctest.testfile(examples + "/frame/bin_column.rst", module_relative=False, verbose=True, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)