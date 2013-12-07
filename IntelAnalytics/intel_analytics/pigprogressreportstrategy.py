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
import re

from intel_analytics.report import ReportStrategy, get_pig_progress
from progress import Progress

job_completion_pattern = re.compile(r".*?MapReduceLauncher - 100% complete")

class PigProgressReportStrategy(ReportStrategy):
    def __init__(self):
        # show this bar for initialization
        self.initialization_progressbar = Progress("Initialization")
        self.initialization_progressbar._repr_html_()
        self.initialization_progressbar._enable_animation()
        self.initialization_progressbar.update(100)

        self.progress_bar = None
                
    def report(self, line):
        progress = get_pig_progress(line)

        if progress:
            if not self.progress_bar:
                self.initialization_progressbar._disable_animation()
                progress_bar = Progress("Progress")
                progress_bar._enable_animation()
                progress_bar._repr_html_()
                self.progress_bar = progress_bar

            self.progress_bar.update(progress)
            
        if self._is_computation_complete(line):
            self.progress_bar._disable_animation()

    def _is_computation_complete(self, line):
        match = re.match(job_completion_pattern, line)
        if match:
            return True
        else:
            return False

    def handle_error(self, error_code, error_message):
        if not self.progress_bar:
            self.initialization_progressbar.alert()
        else:
            self.progress_bar.alert()



