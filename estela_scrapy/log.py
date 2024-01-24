import logging
import os
import sys
import time
import warnings

from estela_queue_adapter import queue_noisy_libraries
from twisted.python import log as txlog

from estela_scrapy.utils import producer, to_standard_str

_stderr = sys.stderr


def _logfn(level, message, parent="none"):
    data = {
        "jid": os.getenv("ESTELA_SPIDER_JOB"),
        "payload": {"log": str(message), "datetime": float(time.time())},
    }
    producer.send("job_logs", data)


def init_logging():
    # General python logging
    root = logging.getLogger()
    root.setLevel(
        logging.NOTSET
    )  # NOSET Make processing all messages if is set in root

    hdlr = LogHandler()
    hdlr.setLevel(logging.INFO)
    hdlr.setFormatter(logging.Formatter("[%(name)s] %(message)s"))
    root.addHandler(hdlr)

    # Silence commonly used noisy libraries
    nh = logging.NullHandler()
    for ln in ["requests", "py.warnings"] + queue_noisy_libraries:
        lg = logging.getLogger(ln)
        lg.propagate = 0
        lg.addHandler(nh)

    # Redirect standard output and error
    sys.stdout = StdoutLogger(False, "utf-8")
    sys.stderr = StdoutLogger(True, "utf-8")

    # Twisted specifics (includes Scrapy)
    obs = LogObserver(hdlr)
    _oldshowwarning = warnings.showwarning
    txlog.startLoggingWithObserver(obs.emit, setStdout=False)
    warnings.showwarning = _oldshowwarning
    return hdlr


class LogHandler(logging.Handler):
    """Python logging handler"""

    def emit(self, record):
        try:
            message = self.format(record)
            if message:
                _logfn(message=message, level=record.levelno, parent="LogHandler")
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def handleError(self, record):
        cur = sys.stderr
        try:
            sys.stderr = _stderr
            super(LogHandler, self).handleError(record)
        finally:
            sys.stderr = cur


class LogObserver(object):
    """Twisted log observer"""

    def __init__(self, loghdlr):
        self._hs_loghdlr = loghdlr

    def emit(self, ev):
        logitem = self._get_log_item(ev)
        if logitem:
            _logfn(**logitem, parent="LogObserver")

    def _get_log_item(self, ev):
        """Get logs from scrapy in his level or Info level in other case"""
        if ev["system"] == "scrapy":
            level = ev["logLevel"]
        else:
            if ev["isError"]:
                level = logging.ERROR
            else:
                level = logging.INFO

        # Ignore levels
        if level < self._hs_loghdlr.level:
            return

        msg = ev.get("message")
        if msg:
            msg = to_standard_str(msg[0])

        failure = ev.get("failure", None)
        if failure:
            msg = failure.getTraceback()

        why = ev.get("why", None)
        if why:
            msg = "{}\n{}".format(why, msg)

        fmt = ev.get("format")
        if fmt:
            try:
                msg = fmt.format(ev)
            except:
                msg = "FAILED TO APPLY FORMAT: fmt={} ev={}".format(repr(fmt), repr(ev))
                level = logging.ERROR

        msg = msg.replace("\n", "\n\t")
        return {"message": msg, "level": level}


class StdoutLogger(txlog.StdioOnnaStick):
    """Catch logs from sterr and stdout"""

    def __init__(self, isError=False, encoding=None, loglevel=logging.INFO):
        txlog.StdioOnnaStick.__init__(self, isError, encoding)
        self.prefix = "[stderr] " if isError else "[stdout] "
        self.loglevel = loglevel

    def _logprefixed(self, msg):
        _logfn(message=self.prefix + msg, level=self.loglevel, parent="StdoutLogger")

    def write(self, data):
        data = to_standard_str(data, self.encoding)

        d = (self.buf + data).split("\n")
        self.buf = d[-1]
        messages = d[0:-1]
        for message in messages:
            self._logprefixed(message)

    def writelines(self, lines):
        for line in lines:
            line = to_standard_str(line, self.encoding)
            self._logprefixed(line)
