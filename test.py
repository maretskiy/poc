#!/user/bin/env python3.6

import os
import random
import sys
import time


def main():
    testables = [
        Dummy(),
        PyList(),
        Logger(),
        SQliteBuffered(),
        SQliteSingleTransaction(),
        SQlite()
    ]
    records_count = 100000

    for testable in testables:
        testable.run_benchmark(records_count)


class Testable:
    def __init__(self):
        self.storage = self.setup_storage()

    def setup_storage(self):
        """Init storage for records."""

    def save_record(self, record):
        """Save single record."""

    def finish(self):
        """Optional final actions."""

    def run_benchmark(self, recourds_count):
        i = 0
        start_ts = time.time()

        while i < recourds_count:
            i += 1
            record = "{}: INFO: {}".format(time.time() - 1519720000, random.random())
            self.save_record(record)
        self.finish()

        duration = time.time() - start_ts

        if type(self.storage) == str:
            props = ['file', os.path.basename(self.storage), os.path.getsize(self.storage)]
        else:
            props = ['memory', type(self.storage).__name__, sys.getsizeof(self.storage)]
        format_args = [self.__class__.__name__, i, duration] + props
        print("{:26} | {} records | {:8.3f} seconds | {:6} | {:16} | {} bytes".format(*format_args))

    def setup_file(self, filename):
        dirname = os.path.dirname(os.path.abspath(__file__))
        filepath = os.path.join(dirname, filename)
        if os.path.isfile(filepath):
            os.remove(filepath)
        return filepath


class Dummy(Testable):
    """Do nothing."""


class PyList(Testable):
    """Save records in python runtime list."""
    def setup_storage(self):
        return []

    def save_record(self, record):
        self.storage.append(record)


class Logger(Testable):
    """Standard logging into file."""
    def setup_storage(self):
        import logging
        log_file = self.setup_file('tmp.log')
        self.logger = logging.getLogger('test')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.FileHandler(log_file))
        return log_file

    def save_record(self, record):
        self.logger.info(record)


class SQlite(Testable):
    """Transaction per each record."""
    def setup_storage(self):
        import sqlite3
        db_file = self.setup_file('tmp.sqlite')
        self.con = sqlite3.connect(db_file)
        with self.con:
            self.con.execute("CREATE TABLE log (ts real, level integer, message varchar)")
        return db_file

    def save_record(self, record):
        with self.con:
            self.con.execute("INSERT INTO log (ts, level, message) VALUES (?, ?, ?)", (time.time(), 1, record))


class SQliteSingleTransaction(Testable):
    """Save all records in one transaction."""
    def setup_storage(self):
        import sqlite3
        db_file = self.setup_file('tmp.sqlite_st')
        self.con = sqlite3.connect(db_file)
        with self.con:
            self.con.execute("CREATE TABLE log (ts real, level integer, message varchar)")
        self.cur = self.con.cursor()
        return db_file

    def save_record(self, record):
        self.cur.execute("INSERT INTO log (ts, level, message) VALUES (?, ?, ?)", (time.time(), 1, record))

    def finish(self):
        self.con.commit()
        self.con.close()


class SQliteBuffered(Testable):
    """Buffer records and save them periodically."""
    def setup_storage(self):
        import sqlite3
        db_file = self.setup_file('tmp.sqlite_buf')
        self.con = sqlite3.connect(db_file)
        with self.con:
            self.con.execute("CREATE TABLE log (ts real, level integer, message varchar)")
        self.cur = self.con.cursor()
        self.buffer = []
        self.limit = 100
        return db_file

    def save_record(self, record):
        self.buffer.append((time.time(), 1, record))
        if len(self.buffer) >= self.limit:
            self._flush_buffer()

    def finish(self):
        self._flush_buffer()
        self.con.close()

    def _flush_buffer(self):
        if self.buffer:
            for record in self.buffer:
                self.cur.execute("INSERT INTO log (ts, level, message) VALUES (?, ?, ?)", record)
            self.con.commit()
            self.buffer.clear()


if __name__ == '__main__':
    main()
