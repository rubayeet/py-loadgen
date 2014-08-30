__author__ = 'imyousuf'

import math
import datetime

__BASE = datetime.datetime.fromtimestamp(0)

def get_current_time_in_millis():
    a = datetime.datetime.now()
    c = a - __BASE
    return int((c.days * 24 * 60 * 60 + c.seconds) * 1000 + c.microseconds / 1000.0)

class ExecutionStat(object):
    def __init__(self, group_name, start_time = 0, end_time = 0):
        self._start_time = start_time
        self._end_time = end_time
        self._group_name = group_name

    def start(self):
        self._start_time = get_current_time_in_millis()
        self._end_time = self._start_time

    def stop(self):
        self._end_time = get_current_time_in_millis()

    @property
    def duration(self):
        return self._end_time - self._start_time

    @property
    def group_name(self):
        return self._group_name

    def __str__(self):
        started_on = datetime.datetime.fromtimestamp(self._start_time/1000).strftime("%A, %m/%d/%Y %H:%M")
        return str("Duration of query '%s' executed on %s is %d" %(self._group_name, started_on, int(self.duration)))

class ExecutionResult(object):
    def __init__(self):
        self._average = 0.0
        self._std_deviation = 0.0
        self._max = 0
        self._min = 0

    @property
    def average(self):
        return self._average
    @property
    def std_deviation(self):
        return self._std_deviation
    @property
    def max(self):
        return self._max
    @property
    def min(self):
        return self._min
    @average.setter
    def average(self, avg):
        self._average = avg
    @std_deviation.setter
    def std_deviation(self, std_dev):
        self._std_deviation = std_dev
    @max.setter
    def max(self, max):
        self._max = max
    @min.setter
    def min(self, min):
        self._min = min
    def __str__(self):
        return "min/max/avg/std-dev/90percentile. - %s/%s/%s/%s/%s" % (self._min, self._max, self._average, self._std_deviation, self._percentile_90)


class StatisticsCollector(object):
    def __init__(self):
        self._stats = []

    def add(self, execution_stat):
        self._stats.append(execution_stat)

    def get_comprehensive_summary(self):
        """
        Compute the test summary based on all execution regardless of the group the execution belongs to.
        :return: The overall test summary
        """
        return _compute_result(self._stats)

    def get_summary_per_query(self):
        """
        Find all unique groups in the statistics collected and group results by that name
        :return: The load test result grouped by statistics/execution group name in a dictionary
        """
        result = dict()
        group_stats = dict()
        for stat in self._stats:
            if not group_stats.has_key(stat.group_name):
                group_stats[stat.group_name] = []
            group_stats[stat.group_name].append(stat)
        for k, v in group_stats.items():
            result[k] = _compute_result(v)
        return result

def _compute_result(stats):
    result = ExecutionResult()
    total = 0
    for stat in stats:
        duration = stat.duration
        if result.max < duration:
            result.max = duration
        if result.min > duration:
            result.min = duration
        total += duration
    result.average = total / len(stats)
    difference_2_sum = 0
    for stat in stats:
        duration = stat.duration
        difference_2_sum += ((duration - result.average) ** 2)
    average_difference_2 = difference_2_sum/len(stats)
    result.std_deviation = average_difference_2 ** 0.5
    result._percentile_90 = percentile(sorted([stat.duration for stat in stats]), 0.9)
    return result
    
def percentile(N, percent):
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        returnN[int(k)]
    d0 = N[int(f)] * (c-k)
    d1 = N[int(c)] * (k-f)
    return d0+d1
