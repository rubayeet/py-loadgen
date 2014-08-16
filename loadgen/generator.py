__author__ = 'imyousuf'

from threading import Thread
from Queue import Queue
from abc import ABCMeta, abstractmethod
from pymongo import MongoClient
from stat import ExecutionStat
from . import collector

job_queue = Queue()

class JobGenerator(object):
    __metaclass__ = ABCMeta
    @abstractmethod
    def next_job(self, configuration):
        raise NotImplementedError()

class Job(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_group_name(self):
        raise NotImplementedError()

    @abstractmethod
    def execute_job(self):
        raise NotImplementedError()

    def run(self):
        stat = ExecutionStat(self.get_group_name())
        stat.start()
        self.execute_job()
        stat.stop()
        collector.add(stat)

class MongoQueryExecutor(Job):
    def __init__(self, connection, query_name, query_string):
        self._connection = connection
        self._query_name = query_name
        self._query_string = query_string

    def get_group_name(self):
        return self._query_name

    def execute_job(self):
        exe_c = '%s.%s' % ('self._connection', self._query_string)
        print 'Executing ', exe_c
        eval(exe_c)

class MongoDBQueryJobGenerator(JobGenerator):

    def __init__(self):
        self._connection = None

    def next_job(self, configuration):
        if not self._connection:
            print 'Connecting to ', configuration.connection_string
            self._connection = MongoClient(configuration.connection_string)
        (qn, qs) = configuration.get_random_query()
        return MongoQueryExecutor(self._connection, qn, qs)

def worker():
    while True:
        item = job_queue.get()
        item.run()
        job_queue.task_done()

def generate_load(configuration, job_generator = MongoDBQueryJobGenerator):
    for i in range (configuration.concurrent_requests):
        t = Thread(target=worker)
        t.daemon = True
        t.start()
    total = configuration.concurrent_requests * configuration.runs_per_thread
    print "Jobs to be generated are ", total
    job_gen = job_generator()
    for j in range(total):
        job_queue.put(job_gen.next_job(configuration))
    job_queue.join()
    return collector
