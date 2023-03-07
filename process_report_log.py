###############################################################
# Script for extracting logging info for Synda's download queue
# from the weekly reports log.
#
# This script generates a JSON file with stats on file count
# and storage size of the Synda database grouped by status.
# Stats are organized by the date and time they were generated.
###############################################################

import re
import json

timestamp_pattern = r"""(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\nsynda queue\n(.*?)status\s+count\s+size\n(.*?)/scripts/synda-perf.py"""

log_file = "/var/log/synda/reports.log"

log_str = open(log_file, 'r').read()

log_entries = re.findall(timestamp_pattern, log_str, re.DOTALL|re.MULTILINE)

if log_entries is not None:
    log_dict = {}
    for l in log_entries:
        timestamp = l[0]
        synda_section = l[2]
        synda_queue_pattern = r"""(.*?)\s+(.*?)\s+(.*? (Byte|Bytes|kB|MB|GB|TB|PB|EB|ZB|YB))\n"""
        synda_queue_match = re.findall(synda_queue_pattern, synda_section, re.DOTALL|re.MULTILINE)

        log_dict[timestamp] = {s[0]:dict(file_count=int(s[1]), size=s[2]) for s in synda_queue_match}
        
    with open('synda_queue_stats.json', 'w') as stats_file:
        stats_file.write(json.dumps(dict(synda_queue_stats=log_dict), indent=4))