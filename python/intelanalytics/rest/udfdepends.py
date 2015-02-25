//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

import os
import sys
import intelanalytics.rest.spark


class udf(object):
    """
    An object for containing  and managing all the UDF dependencies.

    """
    @staticmethod
    def install(dependencies):
        """
        installs the specified files on the cluster

        Notes
        -----
        The files to install can be:
        either local python scripts without any packaging structure
        e.g.: ia.Udf.install(['my_script.py']) where my_script.py is in the same local folder
        or
        absolute path to a valid python package/module which includes the intended python file and all its
        dependencies.
        e,g: ia.Udf.install(['testcases/auto_tests/my_script.py'] where it is essential to install the whole 'testcases'
        module on the worker nodes as 'my_script.py' has other dependencies in the 'testcases' module. There are certain
        pitfalls associated with this approach:
        In this case all the folders, subfolders and files within 'testcases' directory get zipped, serialized and
        copied over to each of the worker nodes every time that the user calls a function that uses UDFs.

        So, to keep the overhead low, it is strongly advised that the users make a separate 'utils' folder and keep all
        their python libraries in that folder and simply install that folder to the workers.
        Also, when you do not need the dependencies for future function calls, you can prevent them from getting copied
        over every time by doing ia.Udf.install([]), to empty out the install list.
        This approach does not work for imports that use relative paths.
        
        :param dependencies: the file dependencies to be serialized to the cluster
        :return: nothing
        """
        if dependencies is not None:
            intelanalytics.rest.spark.UdfDependencies = dependencies
        else:
            raise ValueError ("The dependencies list to be installed on the cluster cannot be empty")

    @staticmethod
    def list():
        """
        lists all the user files getting copied to the cluster
        :return: list of files to be copied to the cluster
        """
        return intelanalytics.rest.spark.UdfDependencies
