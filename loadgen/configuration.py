__author__ = 'imyousuf'

import ConfigParser

class Configuration(object):
    def __init__(self):
        self._connection_string = ''
        self._queries = dict()
        self._concurrent_requests = 10
        self._runs_per_thread = 10
        self._explain_each_query = False

    @property
    def connection_string(self):
        return self._connection_string
    @connection_string.setter
    def connection_string(self, connxn_str):
        self._connection_string = connxn_str
    @property
    def queries(self):
        return self._queries
    def add_query(self, name, query_string):
        self._queries[name] = query_string
    def remove_query(self, name):
        del self._queries[name]
    @property
    def concurrent_requests(self):
        return self._concurrent_requests
    @concurrent_requests.setter
    def concurrent_requests(self, creq):
        self._concurrent_requests = creq
    @property
    def runs_per_thread(self):
        return self._runs_per_thread
    @runs_per_thread.setter
    def runs_per_thread(self, rpt):
        self._runs_per_thread = rpt
    @property
    def explain_each_query(self):
        return self._explain_each_query
    @explain_each_query.setter
    def explain_each_query(self, eeq):
        self._explain_each_query = eeq

    def __str__(self):
        return str("Generate load with %d conncurrent threads and execute one random query at a time for %d times \
against the %s DB Connection and at the end dump the explanation of the queries - %s"
                       % (int(self._concurrent_requests), int(self._runs_per_thread), self._connection_string, self._explain_each_query))
    def __unicode__(self):
        return unicode("Generate load with %d conncurrent threads and execute one random query at a time for %d times \
against the %s DB Connection and at the end dump the explanation of the queries - %s"
                       % (self._concurrent_requests, self._runs_per_thread, self._connection_string, self._explain_each_query))

def parse_configuration(configuration):
    if isinstance(configuration, ConfigParser.ConfigParser):
        conf = Configuration()
        conf.connection_string = configuration.get('init', 'connection_string')
        for query_conf in configuration.items('queries'):
            conf.add_query(query_conf[0], query_conf[1])
        if configuration.has_section('load'):
            conf.concurrent_requests = configuration.get('load', 'concurrent', 10)
            conf.runs_per_thread = configuration.get('load', 'runs_per_thread', 10)
            conf.explain_each_query = configuration.get('load', 'explain_each_query') == 'true'
        else:
            conf.concurrent_requests = 10
            conf.runs_per_thread = 10
            conf.explain_each_query = False
        return conf
    return None
