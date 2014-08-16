#!/usr/bin/python
__author__ = 'imyousuf'
import ConfigParser, sys
from loadgen import configuration, generator

def main():
    """
    Script for generating load to an MongoDB Instance
    """
    print "Generate Load to MongoDB"
    config = ConfigParser.SafeConfigParser()
    config_path = None
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = 'connection.config'
    config.read(config_path)
    if not config.has_section('init'):
        raise IOError('"init" section must be defined with "connection_string" configuration')
    if not config.has_section('queries'):
        raise IOError('No use executing the script without queries')
    if not config.has_section('load'):
        print "WARNING: No load generation configuration set, will be using defaults"
    if len(config.get('init', 'connection_string', '')) <= 0:
        raise IOError('Connection string (connection_string) not provided')
    stats_collector = generator.generate_load(configuration.parse_configuration(config))
    print stats_collector.get_comprehensive_summary()
    q_dict = stats_collector.get_summary_per_query()
    for (q, s) in q_dict.iteritems():
        print "Summary for %s - %s" %(q, s)

if __name__ == "__main__":
    main()
