#!/usr/bin/env python3

import json
import random
import time

import perf


def main():
    testables = (Dummy, JsonLoads, JsonDumps, TsvLoads, TsvDumps, TsvLoadsV2, TsvDumpsV2,
                 TsvLoadsV3, TsvDumpsV3)
    print(perf.Benchmark(testables, 300000).as_text())


tsv_value_types = {
    'int': int,
    'float': float,
    'str': str,
}


def from_tsv(tsv):
    r = []
    try:
        for i in tsv.split('\t'):
            t, v = i.split(':', 1)
            r.append((t, tsv_value_types[t](v)))
        return r
    except KeyError as e:
        raise AssertionError(f"unexpected type {e} in '{tsv}'")


def to_tsv(*items):
    if not items:
        return ''
    tsv = '\t'.join((f'{t}:{v}' for t, v, in items))
    assert len([i for i in tsv if i == '\t']) < len(items), f"TAB found in {items}"
    return tsv


def from_tsv_v2(tsv):
    r = []
    for i in tsv.split('\t'):
        t, v = i.split(':', 1)
        r.append(tsv_value_types[t](v))
    return r


def to_tsv_v2(*items):
    return '\t'.join(f'{t}:{v}' for t, v, in items)


def from_tsv_v3(tsv):
    return tsv.split('\t')


def to_tsv_v3(*items):
    return '\t'.join(str(i) for i in items)


class Dummy(perf.Testable):
    """Just for comparison."""

    def action(self):
        return 'foo', time.time(), 'bar', random.random()


class JsonLoads(perf.Testable):

    def action(self):
        return json.loads(f'["foo", {time.time()}, "bar", {random.random()}]')


class JsonDumps(perf.Testable):

    def action(self):
        return json.dumps(('foo', time.time(), 'bar', random.random()))


class TsvLoads(perf.Testable):

    def action(self):
        return from_tsv(f'str:foo\tfloat:{time.time()}\tstr:bar\tfloat:{random.random()}')


class TsvDumps(perf.Testable):

    def action(self):
        return to_tsv(('int', 'foo'), ('float', time.time()), ('str', 'bar'),
                      ('float', random.random()))


class TsvLoadsV2(perf.Testable):

    def action(self):
        return from_tsv_v2(f'str:foo\tfloat:{time.time()}\tstr:bar\tfloat:{random.random()}')


class TsvDumpsV2(perf.Testable):

    def action(self):
        return to_tsv_v2(('int', 'foo'), ('float', time.time()), ('str', 'bar'),
                         ('float', random.random()))


class TsvLoadsV3(perf.Testable):

    def action(self):
        return from_tsv_v3(f'foo\tbar\t{time.time()}\tspam\t{random.random()}')


class TsvDumpsV3(perf.Testable):

    def action(self):
        return to_tsv_v3('foo', 'bar', time.time(), 'spam', random.random())


if __name__ == '__main__':
    main()
