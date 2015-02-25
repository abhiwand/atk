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

"""
This file zips and converts necessary modules to evaluate a lambda expression by looking into UdfDependencies list
"""

import zipfile
import os
import os.path

class UdfZip(object):

    # From http://stackoverflow.com/questions/14438928/python-zip-a-sub-folder-and-not-the-entire-folder-path
    @staticmethod
    def _dirEntries(dir_name, subdir, *args):
        # Creates a list of all files in the folder
        '''Return a list of file names found in directory 'dir_name'
        If 'subdir' is True, recursively access subdirectories under 'dir_name'.
        Additional arguments, if any, are file extensions to match filenames. Matched
            file names are added to the list.
        If there are no additional arguments, all files found in the directory are
            added to the list.
        Example usage: fileList = dirEntries(r'H:\TEMP', False, 'txt', 'py')
            Only files with 'txt' and 'py' extensions will be added to the list.
        Example usage: fileList = dirEntries(r'H:\TEMP', True)
            All files and all the files in subdirectories under H:\TEMP will be added
            to the list. '''

        fileList = []
        for file in os.listdir(dir_name):
            dirfile = os.path.join(dir_name, file)
            if os.path.isfile(dirfile):
                if not args:
                    fileList.append(dirfile)
                else:
                    if os.path.splitext(dirfile)[1][1:] in args:
                        fileList.append(dirfile)
                        # recursively access file names in subdirectories
            elif os.path.isdir(dirfile) and subdir:
                fileList.extend(UdfZip._dirEntries(dirfile, subdir, *args))
        return fileList

    @staticmethod
    def _makeArchive(fileList, archive, root):
        """
        'fileList' is a list of file names - full path each name
        'archive' is the file name for the archive with a full path
        """
        with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for f in fileList:
                zipf.write(f, os.path.relpath(f, root))

    @staticmethod
    def zipdir(path):
        # zips a path to /tmp/iapydependencies.zip. Please note that this zip file will be truncated every time
        # this call is made. So to preserver the contents, read the file immediately or copy. Not thread-safe.
        UdfZip._makeArchive(UdfZip._dirEntries(path, True), '/tmp/iapydependencies.zip', path[0:path.rfind('/')])
