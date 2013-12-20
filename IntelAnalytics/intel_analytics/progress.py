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
import uuid
from IPython.display import HTML, Javascript, display


class Progress:
    def __init__(self, name, max_value=100):
        self.name = name
        self.id = str(uuid.uuid4())
        self.max_value = max_value
        self.value = 0
        self.is_in_alert = False
        self.is_in_animation = False

    def _repr_html_(self):
        pb = HTML(
            """
            <div id="%s" class='status'>
                <span class="label span2">%s</span>
                <span class="span7 progress">
                      <div class="bar" style= "width:0%%">&nbsp;</div>
                </span>
            </div>
            """ % (self.id, self.name))
        display(pb)
        return pb

    def update(self, value):
        self.value = value
        display(Javascript("$('#%s div').first().width('%i%%')" %
                           (self.id, (float(value) / float(self.max_value)) * 100)))
        if (value == self.max_value) and (not self.is_in_animation) and (not self.is_in_alert):
            self._update_text()

    def delete(self):
        display(Javascript("$('#%s div').first().parents('.status').remove()" % self.id))

    def _enable_animation(self):
        display(Javascript("$('#%s span').last().addClass(\"progress-striped active\")" % self.id))
        self.is_in_animation = True

    def _disable_animation(self):
        display(Javascript("$('#%s span').last().removeClass(\"progress-striped active\")" % self.id))
        if not self.is_in_alert:
            self._update_text()
        self.is_in_animation = False

    def alert(self):
        self.is_in_alert = True
        self._update_text()
        self._disable_animation()
        display(Javascript("$('#%s span').last().addClass(\"progress-danger\")" % self.id))

    def _update_text(self):
        """
        update the title of progress bar when the step is completed.
        """
        if self.name == 'Progress':
            if self.is_in_alert:
                self.name = 'Execution failed'
            else:
                self.name = 'Execution completed'
        else:
            if self.is_in_alert:
                self.name = self.name + ' failed'
            else:
                self.name = self.name + ' completed'

        display(Javascript("""$('#%s span').first().text('%s')""" % (self.id, self.name)))
