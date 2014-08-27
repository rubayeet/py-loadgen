#!/usr/bin/python
__author__ = 'imyousuf'
import ConfigParser, sys
from loadgen import configuration, generator

def main():
    """
    Script for generating load to systems
    """
    print "Generate Load"
    config = ConfigParser.RawConfigParser()
    config_path = None
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = 'load_generation.config'
    config.read(config_path)
    if not config.has_section('base'):
        raise IOError('"base" section must be defined with "target_type" configuration')
    if not config.has_section('load'):
        print "WARNING: No load generation configuration set, will be using defaults"
    if not config.has_option('base', 'target_type') or len(config.get('base', 'target_type')) <= 0:
        raise IOError('Load target type (target_type) not provided')
    stats_collector = generator.generate_load(configuration.parse_configuration(config))
    print stats_collector.get_comprehensive_summary()
    q_dict = stats_collector.get_summary_per_query()
    for (q, s) in q_dict.iteritems():
        print "Summary for %s - %s" %(q, s)

if __name__ == "__main__":
    main()
