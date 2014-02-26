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
"""
Invokes subprocess calls with polling to check progress.
"""
import types

import time
import re

from threading import Thread
from subprocess import PIPE, Popen
from intel_analytics.report import JobReportService

SIGTERM_TO_SIGKILL_SECS = 2 # seconds to wait before send the big kill

def call(args, report_strategy=None, heartbeat=0, timeout=0, shell=False, return_stdout=0):
    """
    Runs the command described by args in a subprocess, with or without polling.

    Starts a subprocess, which runs the command described by args.  It consumes
    the subprocess's STDERR and collects its return code.  If called with a
    heartbeat N, then this function will poll the subprocess every N seconds
    to see if the command has completed.

    When the subprocess completes, if the return code != 0 then an Exception is
    raised containing the return code and the STDERR collected from the
    subprocess.

    Parameters
    ----------

    args : A list of strings describing the command.

    heartbeat : If > 0, then poll every hb seconds.
    report_strategy: ReportStrategy


    timeout : If > 0, then raise an Exception if execution of the cmd exceeds
        that many seconds.
    """

    # A non-blocking invocation of the subprocess.
    p = Popen(args, shell=shell, stderr=PIPE, stdout=PIPE)
    report_service = JobReportService()
    if isinstance(report_strategy, types.ListType):
        for strategy in report_strategy:
            report_service.add_report_strategy(strategy)
    else:
        report_service.add_report_strategy(report_strategy)

    # Spawns a thread to consume the subprocess's STDERR in a non-blocking manner.
    err_txt = []
    te = Thread(target=_process_error_output, args=(p.stderr, err_txt, report_service))
    te.daemon = True  # thread dies with the called process
    te.start()

    out_txt = []
    to = Thread(target=_report_output, args=(p.stdout, out_txt, report_service, return_stdout))
    to.daemon = True # thread dies with the called process
    to.start()

    rc = None
    if heartbeat > 0:
        # Polls at heartbeat interval.
        rc = p.poll()
        countdown = timeout
        while(rc is None):
            time.sleep(heartbeat)
            countdown -= heartbeat
            if countdown == 0:
                _timeout_abort(p, ' '.join(args), timeout)

            rc = p.poll()
    else:
        rc = p.wait() # block on subprocess.

    # wait for thread to finish in no more than 10 seconds
    te.join(10)
    to.join(10)

    #there is case where rc = 0 when stderr occurs
    if rc != 0 or len(err_txt) > 0:
        msg = ''.join(err_txt) if len(err_txt) > 0 else "(no msg provided)"
        if return_stdout > 0:
            report_service.handle_error(rc, msg)
            print msg
        else:
            if rc != 0:
                report_service.handle_error(rc, msg)
                print rc, msg
    #    raise Exception("Error {0}: {1}".format(rc,msg))

    if return_stdout > 0:
        if len(out_txt) > 0:
            output = ' '.join(out_txt)
            return output.split('\n')
        else:
            return ''
    else:
        return rc


def _report_output(out, string_list, report_service, return_stdout):
    for line in iter(out.readline, b''):
        report_service.report_line(line)
        if return_stdout > 0:
            string_list.append(line)
    out.close()

def _process_error_output(out, string_list, report_service):
    """
    Continously reads from the stream and appends to a list of strings.
    """
    for line in iter(out.readline, b''):
        report_service.report_line(line)
        string_list.append(line)
    out.close()

def _timeout_abort(process, cmd, timeout):
    """
    Attempts to kill the process (first SIGTERM, then SIGKILL) and raises
    a timeout exception.
    """
    process.terminate()
    signals = "SIGTERM"
    time.sleep(SIGTERM_TO_SIGKILL_SECS)
    if process.poll() is None:
        process.kill()
        signals += " and SIGKILL"
    raise Exception("TIMEOUT {0} seconds, sent {1} to {2} {3}"
    .format(timeout, signals, process.pid, cmd))

