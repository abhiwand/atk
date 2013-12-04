"""
Calls "subprocess".
"""

import time

from threading import Thread
from subprocess import PIPE, Popen

SIGTERM_TO_SIGKILL_SECS = 2 # seconds to wait before send the big kill

def call(args, heartbeat=0, func=None, timeout=0, shell=False):
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

    func : If heartbeat > 0, then this method is called at every heartbeat.

    timeout : If > 0, then raise an Exception if execution of the cmd exceeds
        that many seconds.
    """

    # A non-blocking invocation of the subprocess.
    p = Popen(args, shell=shell, stderr=PIPE)

    # Spawns a thread to consume the subprocess's STDERR in a non-blocking manner.
    err_txt = []
    t = Thread(target=_append_output, args=(p.stderr, err_txt))
    t.daemon = True # The thread dies with the called process.
    t.start()

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
            if func is not None:
                func()
            rc = p.poll()
    else:
        rc = p.wait() # block on subprocess.

    if rc != 0:
        msg = ''.join(err_txt) if len(err_txt) > 0 else "(no msg provided)"
        raise Exception("Error {0}: {1}".format(rc,msg))


def _append_output(out, list):
    """
    Continously reads from the stream and appends to a list of strings.
    """
    for line in iter(out.readline, b''):
        list.append(line)
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

