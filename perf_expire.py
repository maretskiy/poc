#!/usr/bin/env python3.6

import collections
import random
import sys
import time


def main():
    testables = [
        Dummy(),
        DequeLog(),
        DoubleLinked()
    ]

    print("Test             Sessions   MemoryUse  ActivitiesDur  CleanupSessions  CleanupDur")
    print("--------------  ---------   ---------  -------------  ---------------  ----------")

    for testable in testables:
        testable.run_benchmark()


class Testable:
    USER_ACTIVITIES_NUM = 1000000
    EXPIRE_PERIOD_RATIO = .5

    def __init__(self):
        self._ts = 0
        self.user_sessions = {}
        self.setup()

    def timestamp(self):
        self._ts += 1
        return self._ts

    def setup(self):
        """Must be overridden."""

    def get_used_memory(self):
        """Must be overridden."""
        return 0

    def optimize_session_search(self, user_id, ts):
        """Must be overridden."""

    def cleanup_sessions(self, expire_timestamp):
        """Must be overridden."""
        return 0

    def run_benchmark(self):
        start_ts = time.time()

        for i in range(self.USER_ACTIVITIES_NUM):
            user_id = random.randint(0, self.USER_ACTIVITIES_NUM)
            ts = self.timestamp()
            self.user_sessions[user_id] = ts
            self.optimize_session_search(user_id, ts)

        activities_duration = time.time() - start_ts

        memory_usage = self.get_used_memory()

        start_ts = time.time()

        expire_timestamp = int(self._ts * self.EXPIRE_PERIOD_RATIO)
        deleted_sessions = self.cleanup_sessions(expire_timestamp)

        cleanup_duration = time.time() - start_ts

        print("{:12}{:13}{:12}{:15}{:17}{:12}".format(type(self).__name__,
                                                      self.USER_ACTIVITIES_NUM,
                                                      memory_usage,
                                                      round(activities_duration, 2),
                                                      deleted_sessions,
                                                      round(cleanup_duration, 2)))


class Dummy(Testable):
    pass


class DequeLog(Testable):
    def setup(self):
        self.activity_log = collections.deque()

    def get_used_memory(self):
        return sys.getsizeof(self.activity_log)

    def optimize_session_search(self, user_id, ts):
        self.activity_log.append((user_id, ts))

    def cleanup_sessions(self, expire_timestamp):
        deleted_sessions = 0
        while self.activity_log:
            user_id, t = self.activity_log.popleft()
            if user_id in self.user_sessions and self.user_sessions[user_id] <= expire_timestamp:
                del self.user_sessions[user_id]
                deleted_sessions += 1
            if t > expire_timestamp:
                break
        return deleted_sessions


class DoubleLinked(Testable):
    def setup(self):
        self.dlset = DoubleLinkedSet()

    def get_used_memory(self):
        return sum([sys.getsizeof(node) for node in self.dlset])

    def optimize_session_search(self, user_id, ts):
        self.dlset.add(user_id, ts)

    def cleanup_sessions(self, expire_timestamp):
        deleted_sessions = 0
        sess = self.dlset.head()
        while sess:
            if sess.value > expire_timestamp:
                break
            self.dlset.unlink(sess.id)
            del self.user_sessions[sess.id]
            deleted_sessions += 1
            sess = self.dlset.head()
        return deleted_sessions


class Node:
    __slots__ = ('id', 'value', 'parent_id', 'child_id')

    def __init__(self, node_id, node_value, parent_id=None, child_id=None):
        self.id = node_id
        self.value = node_value
        self.parent_id = parent_id
        self.child_id = child_id

    def __repr__(self):
        return "<Node {0.id} : {0.value}>".format(self)


class DoubleLinkedSet:
    def __init__(self):
        self._map = {}
        self._head_id = None
        self._tail_id = None

    def add(self, node_id, node_value):
        if node_id in self._map:
            node = self._map.pop(node_id)
            if node.child_id:
                self._map[node.child_id].parent_id = node.parent_id
            if node.parent_id:
                self._map[node.parent_id].child_id = node.child_id
            if node_id == self._head_id:
                self._head_id = node.child_id
            if node_id == self._tail_id:
                self._tail_id = node.parent_id
            node.value = node_value
            node.child_id = None
            node.parent_id = self._tail_id
        else:
            node = Node(node_id, node_value, parent_id=self._tail_id)
        self._map[node_id] = node
        if self._tail_id:
            self._map[self._tail_id].child_id = node_id
        self._tail_id = node_id
        if not self._head_id:
            self._head_id = node_id

    def head(self):
        if self._head_id:
            return self._map[self._head_id]

    def unlink(self, node_id):
        if node_id in self._map:
            node = self._map.pop(node_id)
            if node.child_id:
                self._map[node.child_id].parent_id = node.parent_id
            if node.parent_id:
                self._map[node.parent_id].child_id = node.child_id
            if node_id == self._head_id:
                self._head_id = node.child_id
            if node_id == self._tail_id:
                self._tail_id = node.parent_id

    def __iter__(self):
        node = self._map.get(self._head_id)
        while node:
            next_id = node.child_id
            yield node
            node = self._map.get(next_id)

    def __len__(self):
        return len(self._map)

    def __repr__(self):
        return str(list(self))


if __name__ == '__main__':
    main()
