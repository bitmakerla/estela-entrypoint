import json
import os
import pytest
import threading

from queue import Queue
from bm_scrapy.writer import PipeWriter


@pytest.fixture
def fifo(tmpdir):
    path = os.path.join(str(tmpdir.mkdir('bm')), 'temp.fifo')
    os.mkfifo(path)
    return path


@pytest.fixture
def queue():
    return Queue()


@pytest.fixture
def reader(fifo, queue):
    def read_from_fifo():
        with open(fifo) as f:
            for line in iter(f.readline, ''):
                queue.put(line)

    reader_thread = threading.Thread(target=read_from_fifo)
    reader_thread.start()
    try:
        yield reader_thread
    finally:
        reader_thread.join(timeout=1)

        
@pytest.fixture
def writer(fifo, reader):
    w = PipeWriter(fifo)
    w.open()
    try:
        yield w
    finally:
        w.close()

     
def parse_line(msg):
    assert msg.endswith('\n')
    cmd, _, payload = msg.strip().partition(' ')
    return cmd, json.loads(payload)


def test_close(writer):
    assert writer.pipe.closed is False
    writer.close()
    assert writer.pipe.closed is True
    

def test_write_item(writer, queue):
    writer.write_item({'my': 'item'})
    line = queue.get(timeout=1)
    cmd, payload = parse_line(line)
    assert queue.empty()
    assert cmd == 'ITM'
    assert payload == {'my': 'item'}

    
def test_fin(writer, queue):
    reason = 'boom_reason'
    writer.write_fin(reason)
    line = queue.get(timeout=1)
    cmd, payload = parse_line(line)
    assert queue.empty()
    assert cmd == 'FIN'
    assert payload == {'finish_reason': reason}
