#!/usr/bin/env python3

import collections
import logging
import os
import random
import sqlite3
import time

import perf


def main():
    testables = (Dummy,
                 PyListAppend,
                 PyListPrepend,
                 PyDeque,
                 Logger,
                 AppendToFile,
                 AppendToOpenedFile,
                 SQliteBufferedList,
                 SQliteBufferedReverseList,
                 SQliteBufferedDeque,
                 SQlitePeriodicCommit,
                 SQliteSingleTransaction,
                 # SQlite,
                 SQliteRamSingleTransaction,
                 SQliteRam)
    print(perf.Benchmark(testables, 300000).as_text())


class Testable(perf.Testable):
    storage = None
    buffer_size = 100000

    @classmethod
    def setup_storage(cls, storage):
        if type(storage) == str and storage.startswith('tmp.'):
            dirname = os.path.dirname(os.path.abspath(__file__))
            storage = os.path.join(dirname, storage)
            if os.path.isfile(storage):
                os.remove(storage)
        return storage

    def action_value(self):
        return f"{time.time()}: INFO: {random.random()}"


class Dummy(Testable):
    """Do not store value, just for comparison."""

    def action(self):
        self.action_value()


class PyListAppend(Testable):
    """Save records in python runtime list."""

    def setup(self):
        self.storage = []

    def action(self):
        self.storage.append(self.action_value())


class PyListPrepend(Testable):
    """Save records in python runtime list."""

    def setup(self):
        self.storage = []

    def action(self):
        self.storage.insert(0, self.action_value())


class PyDeque(Testable):
    """Save records in python runtime list."""

    def setup(self):
        self.storage = collections.deque()

    def action(self):
        self.storage.appendleft(self.action_value())


class Logger(Testable):
    """Standard logging into file."""

    def setup(self):
        self.storage = self.setup_storage('tmp.log')
        self.logger = logging.getLogger('test')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.FileHandler(self.storage))

    def action(self):
        self.logger.info(self.action_value())


class AppendToFile(Testable):
    """Append lines to file."""

    def setup(self):
        self.storage = self.setup_storage('tmp.file_to_append')

    def action(self):
        with open(self.storage, 'a') as f:
            f.write(f"{self.action_value()}\n")


class AppendToOpenedFile(Testable):
    """Append lines to opened file."""

    def setup(self):
        self.storage = self.setup_storage('tmp.opened_file_to_append')
        self.f = open(self.storage, 'w')

    def action(self):
        self.f.write(f"{self.action_value()}\n")

    def finalize(self):
        self.f.close()


class SQlite(Testable):
    """Transaction per each record."""
    INSERT = "INSERT INTO log (ts, level, message) VALUES (?, ?, ?)"
    DB = 'tmp.sqlite'

    def setup(self):
        self.storage = self.setup_storage(self.DB)
        self.con = sqlite3.connect(self.storage)
        with self.con:
            self.con.execute("CREATE TABLE log (ts real, level integer, message varchar)")
        self.cur = self.con.cursor()

    def action(self):
        with self.con:
            self.con.execute(self.INSERT, (time.time(), 1, self.action_value()))

    def finalize(self):
        self.con.commit()
        self.con.close()


class SQliteSingleTransaction(SQlite):
    """Save all records in one transaction."""
    DB = 'tmp.sqlite_st'

    def action(self):
        self.cur.execute(self.INSERT, (time.time(), 1, self.action_value()))


class SQliteBufferedList(SQlite):
    """Buffer records and save them periodically."""
    DB = 'tmp.sqlite_list'

    def setup(self):
        super().setup()
        self.buffer = []

    def action(self):
        self.buffer.insert(0, (time.time(), 1, self.action_value()))
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

    def action(self):
        self.buffer.append((time.time(), 1, self.action_value()))
        if len(self.buffer) >= self.buffer_size:
            self._flush_buffer()


class SQliteBufferedDeque(SQlite):
    """Buffer records and save them periodically."""
    DB = 'tmp.sqlite_deque'

    def setup(self):
        super().setup()
        self.buffer = collections.deque()

    def action(self):
        self.buffer.appendleft((time.time(), 1, self.action_value()))
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

    def setup(self):
        super().setup()
        self.buffer = 0

    def action(self):
        self.cur.execute(self.INSERT, (time.time(), 1, self.action_value()))
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
