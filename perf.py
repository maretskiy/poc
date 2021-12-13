import sys
import time

assert sys.version_info.major == 3, "python major version is not 3"
assert sys.version_info.minor >= 5, "python minor version is lower than 5"


class Testable:

    def __init__(self):
        self.setup()

    def setup(self):
        """Custom setup."""

    def action_value(self):
        """Get unique value for single action."""

    def action(self):
        """Do repeated action."""

    def finalize(self):
        """Custom finalize."""


class Benchmark:

    def __init__(self, testables, count):
        self.testables = testables
        self.count = count
        self.result = []

    def run(self):
        result = []
        for t_cls in self.testables:
            t_ins = t_cls()
            start_perf_ts, start_proc_ts = time.perf_counter(), time.process_time()

            for i in range(self.count):
                t_ins.action()
            t_ins.finalize()

            end_perf_ts, end_proc_ts = time.perf_counter(), time.process_time()

            duration_perf = end_perf_ts - start_perf_ts
            duration_proc = end_proc_ts - start_proc_ts

            result.append((t_cls.__name__, t_cls.__doc__ or '', duration_perf, duration_proc))
            sys.stdout.write('.')
            sys.stdout.flush()
        sys.stdout.write('\n')
        self.result = result

    def as_text(self):
        if not self.result:
            self.run()

        r = ["Name                       |   Count   | RealTime | ProcTime | Note",
             "---------------------------|-----------|----------|----------|------------------"]
        for name, doc, dur_perf, dur_proc in self.result:
            r.append(f"{name:26} | {self.count:9} | {dur_perf:8.3f} | {dur_proc:8.3f} | {doc}")
        return '\n'.join(r)
