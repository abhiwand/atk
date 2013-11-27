"""
Invoke subprocess calls with polling to check progress.
"""

import time

from threading import Thread
from subprocess import PIPE, Popen
from intel_analytics.report import JobReportService

SIGTERM_TO_SIGKILL_SECS = 2 # seconds to wait before send the big kill

def call(args, report_strategy=None, heartbeat=0, timeout=0, shell=False):
    """
    Runs the command described by args in a subprocess, with or without polling

    Starts a subprocess which runs the command described by args.  It consumes
    the subprocess's STDERR and collects its return code.  If called with a
    heartbeat N, then this function will poll the subprocess every N seconds
    to see if the command has completed.

    When subprocess completes, if the return code != 0 then an Exception is
    raised containing the return code and the STDERR collected from the
    subprocess.

    Parameters
    ----------

    args : list of strings describing the command

    report_strategy: ReportStrategy

    heartbeat : if > 0, then poll every hb seconds

    timeout : if > 0, then raise an Exception if execution of the cmd exceeds
        that many seconds.
    """

    # non-blocking invocation of subprocess
    p = Popen(args, shell=shell, stderr=PIPE, stdout=PIPE)
    reportService = JobReportService()
    reportService.add_report_strategy(report_strategy)

    # spawn thread to consume subprocess's STDERR in non-blocking manner
    err_txt = []
    te = Thread(target=_process_error_output, args=(p.stderr, err_txt, reportService))
    te.daemon = True # thread dies with the called process
    te.start()
    
    to = Thread(target=_report_output, args=(p.stdout, reportService))
    to.daemon = True # thread dies with the called process
    to.start()

    rc = None
    if heartbeat > 0:
        # poll at heartbeat interval
        rc = p.poll()
        countdown = timeout
        while(rc is None):
            time.sleep(heartbeat)
            countdown -= heartbeat
            if countdown == 0:
                _timeout_abort(p, ' '.join(args), timeout)

            rc = p.poll()
    else:
        rc = p.wait() # block on subprocess

    # wait for thread to finish in no more than 10 seconds
    te.join(10)
    to.join(10)

    if rc != 0:
        msg = ''.join(err_txt) if len(err_txt) > 0 else "(no msg provided)"
        print rc, msg
    #    raise Exception("Error {0}: {1}".format(rc,msg))
    return rc

def _report_output(out, reportService):
    for line in iter(out.readline, b''):
        reportService.report_line(line)
    out.close()

def _process_error_output(out, string_list, reportService):
    """
    continously reads from stream and appends to list of strings
    """
    for line in iter(out.readline, b''):
        reportService.report_line(line)
        string_list.append(line)
    out.close()

def _timeout_abort(process, cmd, timeout):
    """
    Attempts to kill the process (first SIGTERM, then SIGKILL) and raises
    a timeout exception
    """
    process.terminate()
    signals = "SIGTERM"
    time.sleep(SIGTERM_TO_SIGKILL_SECS)
    if process.poll() is None:
        process.kill()
        signals += " and SIGKILL"
    raise Exception("TIMEOUT {0} seconds, sent {1} to {2} {3}"
    .format(timeout, signals, process.pid, cmd))

