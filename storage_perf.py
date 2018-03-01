#!/usr/bin/env python3.6

import collections
import logging
import os
import random
import sqlite3
import sys
import time


def main():
    testables = [
        Dummy,
        PyListAppend,
        PyListPrepend,
        PyDeque,
        Logger,
        SQliteBufferedList,
        SQliteBufferedReverseList,
        SQliteBufferedDeque,
        SQlitePeriodicCommit,
        SQliteSingleTransaction,
        #SQlite,
        SQliteRamSingleTransaction,
        SQliteRam
    ]
    records_count = 300000

    for testable in testables:
        testable.run_benchmark(records_count)


class Testable:
    storage = None
    buffer_size = 100000

    def save_record(self, record):
        """Save single record."""

    def finalize(self):
        """Optional final actions."""

    def setup_storage(self, storage):
        if type(storage) == str and storage.startswith('tmp.'):
            dirname = os.path.dirname(os.path.abspath(__file__))
            storage = os.path.join(dirname, storage)
            if os.path.isfile(storage):
                os.remove(storage)
        return storage

    @classmethod
    def run_benchmark(cls, recourds_count):
        ins = cls()
        i = 0
        start_ts = time.time()

        while i < recourds_count:
            i += 1
            record = "{}: INFO: {}".format(time.time(), random.random())
            ins.save_record(record)
        ins.finalize()

        duration = time.time() - start_ts

        note = "-"
        if type(ins.storage) == str:
            if '/tmp.' in ins.storage:
                note = "file {} {} bytes".format(os.path.basename(ins.storage),
                                                 os.path.getsize(ins.storage))
        elif ins.storage:
            note = "{} {} bytes".format(type(ins.storage).__name__,
                                        sys.getsizeof(ins.storage))
        print("{:26} | {} records | {:7.3f} s | {}".format(type(ins).__name__,
                                                           i, duration, note))


class Dummy(Testable):
    """Do nothing."""


class PyListAppend(Testable):
    """Save records in python runtime list."""
    def __init__(self):
        self.storage = []

    def save_record(self, record):
        self.storage.append(record)


class PyListPrepend(Testable):
    """Save records in python runtime list."""
    def __init__(self):
        self.storage = []

    def save_record(self, record):
        self.storage.insert(0, record)


class PyDeque(Testable):
    """Save records in python runtime list."""
    def __init__(self):
        self.storage = collections.deque()

    def save_record(self, record):
        self.storage.appendleft(record)


class Logger(Testable):
    """Standard logging into file."""
    def __init__(self):
        self.storage = self.setup_storage('tmp.log')
        self.logger = logging.getLogger('test')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.FileHandler(self.storage))

    def save_record(self, record):
        self.logger.info(record)


class SQlite(Testable):
    """Transaction per each record."""
    INSERT = "INSERT INTO log (ts, level, message) VALUES (?, ?, ?)"
    DB = 'tmp.sqlite'

    def __init__(self):
        self.storage = self.setup_storage(self.DB)
        self.con = sqlite3.connect(self.storage)
        with self.con:
            self.con.execute("CREATE TABLE log (ts real, level integer, message varchar)")
        self.cur = self.con.cursor()

    def save_record(self, record):
        with self.con:
            self.con.execute(self.INSERT, (time.time(), 1, record))

    def finalize(self):
        self.con.commit()
        self.con.close()


class SQliteSingleTransaction(SQlite):
    """Save all records in one transaction."""
    DB = 'tmp.sqlite_st'

    def save_record(self, record):
        self.cur.execute(self.INSERT, (time.time(), 1, record))


class SQliteBufferedList(SQlite):
    """Buffer records and save them periodically."""
    DB = 'tmp.sqlite_list'

    def __init__(self):
        super().__init__()
        self.buffer = []

    def save_record(self, record):
        self.buffer.insert(0, (time.time(), 1, record))
        if len(self.buffer) >= self.buffer_size:
            self._flush_buffer()

    def finalize(self):
        self._flush_buffer()
        super().finalize()

    def _flush_buffer(self):
        while self.buffer:
            self.cur.execute(self.INSERT, self.buffer.pop(0))
        self.con.commit()


class SQliteBufferedReverseList(SQliteBufferedList):
    """Buffer records and save them periodically."""
    DB = 'tmp.sqlite_rlist'

    def save_record(self, record):
        self.buffer.append((time.time(), 1, record))
        if len(self.buffer) >= self.buffer_size:
            self._flush_buffer()


class SQliteBufferedDeque(SQlite):
    """Buffer records and save them periodically."""
    DB = 'tmp.sqlite_deque'

    def __init__(self):
        super().__init__()
        self.buffer = collections.deque()

    def save_record(self, record):
        self.buffer.appendleft((time.time(), 1, record))
        if len(self.buffer) >= self.buffer_size:
            self._flush_buffer()

    def finalize(self):
        self._flush_buffer()
        super().finalize()

    def _flush_buffer(self):
        while self.buffer:
            self.cur.execute(self.INSERT, self.buffer.popleft())
        self.con.commit()


class SQlitePeriodicCommit(SQlite):
    """Buffer records and save them periodically."""
    DB = 'tmp.sqlite_deque'

    def __init__(self):
        super().__init__()
        self.buffer = 0

    def save_record(self, record):
        self.cur.execute(self.INSERT, (time.time(), 1, record))
        self.buffer += 1
        if self.buffer >= self.buffer_size:
            self.con.commit()
            self.buffer = 0


class SQliteRam(SQlite):
    """Transaction per each record, in RAM."""
    DB = ':memory:'


class SQliteRamSingleTransaction(SQliteSingleTransaction):
    """Save all records in one transaction, in RAM."""
    DB = ':memory:'


if __name__ == '__main__':
    main()
