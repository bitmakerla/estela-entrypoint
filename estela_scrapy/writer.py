import os
import json
import threading

from estela_scrapy.utils import parse_time


class PipeWriter:
    def __init__(self, fifo_path):
        self.path = fifo_path
        self.lock = threading.Lock()
        self.pipe = None

    def open(self):
        try:
            with self.lock:
                self.pipe = open(self.path, "wb")
        except:
            raise RuntimeError

    def close(self):
        with self.lock:
            self.pipe.close()

    def write(self, command, payload):
        command = command.encode("utf-8")
        payload = json.dumps(
            payload,
            separators=(",", ":"),
        ).encode("utf-8")
        with self.lock:
            self.pipe.write(command)
            self.pipe.write(b" ")
            self.pipe.write(payload)
            self.pipe.write(b"\n")
            self.pipe.flush()

    def write_item(self, item):
        self.write("ITM", item)

    def write_request(self, url, status, fp, duration, method, rsize):
        req = {
            "url": url,
            "status": int(status),
            "method": method,
            "duration": int(duration),
            "time": parse_time(),
            "response_size": int(rsize),
            "fingerprint": fp,
        }
        self.write("REQ", req)

    def write_fin(self, reason):
        self.write("FIN", {"finish_reason": reason})


pipe_writer = PipeWriter(os.environ.get("FIFO_PATH", ""))
