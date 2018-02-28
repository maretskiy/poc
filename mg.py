class MgStopCalculation(Exception):
    pass


class Mg(object):

    def __init__(self, num_from, num_to):
        self.__num_from = num_from
        self.__num_to = num_to
        self.__iterations = 0
        self.__actions = [
            #         result, iterations
            lambda i: (i * 2, 1),
            lambda i: (i - 1, 1),
            lambda i: ((i - 1) * 2, 2),
            lambda i: (i * 2, 1),
        ]

    @property
    def num_from(self):
        return self.__num_from

    @property
    def num_to(self):
        return self.__num_to

    def __increment(self, i):
        self.__iterations += i

    @property
    def result(self):
        # Cached
        if self.__iterations:
            return self.__iterations

        try:
            num = self.__num_from
            while True:
                min_delta = None
                for action in self.__actions:
                    num_, iters = action(num)
                    if num_ == self.__num_to:
                        self.__increment(iters)
                        raise MgStopCalculation
                    if (min_delta is None
                        or abs(self.__num_to - num_) < min_delta[0]):
                        min_delta = abs(self.__num_to - num_), iters
                if min_delta:
                    # Got the winner
                    num, iters = min_delta
                    self.__increment(iters)
        except MgStopCalculation:
            return self.__iterations

    @classmethod
    def calculate(cls, num_from, num_to):
        return cls(num_from, num_to).result


import unittest


class MgTestCase(unittest.TestCase):
    def runTest(self):
        # Simple test case
        self.assertEqual(2, Mg.calculate(6, 10))
        self.assertEqual(1, Mg.calculate(5, 10))
        self.assertEqual(1, Mg.calculate(11, 10))
        self.assertEqual(5, Mg.calculate(2, 22))

MgTestCase().runTest()
