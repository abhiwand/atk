##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
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

import abc

class FrameBuilder(object):
    """
   Abstract class for the various table builders to inherit.
   """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def build_from_csv(self, file, schema=None, skip_header=False):
        """
        Reads a CSV (comma-separated-value) file and loads into a table.

        Parameters
        ----------
        C{file} : String
            The path to the file.
        C{schema} : String
            TODO:

        TODO: others parameters for the csv parser

        Returns
        -------
        C{frame} : C{BigDataFrame}
        """
        pass
    @abc.abstractmethod
    def build_from_json(self, file, schema=None):
        """
        Reads a JSON (www.json.org) file and loads it into a table.

        Parameters
        ----------
        C{file} : string
            The path to the file.
        C{schema} : string
            TODO:

        TODO: others parameters for the parser

        Returns
        -------
        C{frame} : BigDataFrame
        """
        pass
    @abc.abstractmethod
    def build_from_xml(self, file, schema=None):
        """
        Reads an XML file and loads it into a table.

        Parameters
        ----------
        C{file} : string
            The path to the file.
        C{schema} : string
            TODO:

        TODO: others parameters for the parser

        Returns
        -------
        C{frame} : C{BigDataFrame}
        """
        pass




