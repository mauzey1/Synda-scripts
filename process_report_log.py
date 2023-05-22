###############################################################
# Script for extracting logging info for Synda's download queue
# from the weekly reports log.
#
# This script generates a JSON file with stats on file count
# and storage size of the Synda database grouped by status.
# Stats are organized by the date and time they were generated.
###############################################################

import re
import os
import sys
import json
import argparse

# get rough estimate of size in bytes from a human-readable size
def human_read_to_bytes(size):
    size_name = ("B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    size = size.split()                
    num, unit = float(size[0]), size[1] 
    if unit in ["Byte", "Bytes"]:
        unit = "B"
    idx = size_name.index(unit)  
    factor = 1000 ** idx               
    return int(num * factor)

def bytes_to_human_read(bytes):
    size_name = ("kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    if bytes == 0:
        return "0 Bytes"
    elif bytes == 1:
        return "1 Byte"
    elif bytes < 1000:
        return f"{bytes} Bytes"
    elif bytes >= 1000:
        for i, unit in enumerate(size_name):
            size = float(bytes) / float(1000 ** (i+1))
            if size < 1000.0:
                break
        return f"{size:.1f} {unit}"

def status_remap(status):
    if status in ["done", "error", "published", "retracted", "waiting", "running"]:
        return status
    elif status == "obsolete":  # "obsolete" files that were replaced with more recent versions
        return "error"
    elif status[:6] == "error-":  # statuses that have "error-" as their prefix
        return "error"
    elif status[-10:] in ",retracted":  # statuses that have ",retracted" as their suffix
        return "retracted"
    else:  # various statuses that only briefly occurred in early replication
        return "miscellaneous"

def process_report_log(log_file, output, db_stats_file=None, remap_statuses=False):

    # Log entries are composed of a timestamp line followed by a line for the command 'synda queue'.
    # The output of 'synda queue' appears between the "status count size" header and a call to the script synda-perf.py.
    timestamp_pattern = r"""(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\nsynda queue\n(.*?)status\s+count\s+size\n(.*?)/scripts/synda-perf.py"""

    log_str = open(log_file, 'r').read()

    log_entries = re.findall(timestamp_pattern, log_str, re.DOTALL|re.MULTILINE)

    if log_entries is not None:
        log_dict = {}
        for l in log_entries:
            timestamp = l[0]
            synda_section = l[2]

            # 'synda queue' reports the file count and storage size (in human-readable decimal units)
            # per download status
            synda_queue_pattern = r"""(.*?)\s+(.*?)\s+(.*? (Byte|Bytes|kB|MB|GB|TB|PB|EB|ZB|YB))\n"""
            synda_queue_match = re.findall(synda_queue_pattern, synda_section, re.DOTALL|re.MULTILINE)

            if remap_statuses:
                statuses = {}
                for s in synda_queue_match:
                    status = status_remap(s[0])
                    file_count = int(s[1])
                    size = human_read_to_bytes(s[2])
                    if status in statuses:
                        statuses[status]['file_count'] += file_count
                        statuses[status]['size'] += size
                    else:
                        statuses[status] = dict(file_count=file_count, size=size)
                for k,v in statuses.items():
                    statuses[k]['size'] = bytes_to_human_read(v['size'])

                log_dict[timestamp] = statuses
            else:
                log_dict[timestamp] = {s[0]:dict(file_count=int(s[1]), size=s[2]) for s in synda_queue_match}
            
            if db_stats_file is not None:
                db_stats_data = json.load(open(db_stats_file,'r'))
                log_dict.update(db_stats_data)
            
            log_dict_sorted = {k: v for k, v in sorted(log_dict.items(), key=lambda item: item[0])}

        with open(output, 'w') as stats_file:
            stats_file.write(json.dumps(log_dict_sorted, indent=4))

if __name__ == '__main__':

    p = argparse.ArgumentParser(
        description="Gather the output of 'synda queue' within the CMIP6 replication's report log" )
    p.add_argument( "--remap_statuses", "-rs", required=False, 
                   action="store_true",
                   help="Remap statuses by combining related statuses into one.  This will also combine the file counts and storage sizes of these statuses." )
    p.add_argument( "--log_file", "-l", required=False,
                    help="Path of log file",
                    default="/var/log/synda/reports.log" )
    p.add_argument( "--db_stats_file", "-ds", required=False,
                    help="Path of JSON file containing database stats made prior to the report log.  Used to supplement the output with past data.",
                    default=None )
    p.add_argument( "--output", "-o", required=False,
                    help="Path of output JSON file",
                    default="synda_queue_stats.json" )

    args = p.parse_args( sys.argv[1:] )

    if not os.path.isfile(args.log_file):
        print("%s is not a file"%(args.log_file))

    if args.db_stats_file is not None and not os.path.isfile(args.db_stats_file):
        print("%s is not a file"%(args.log_file))

    if os.path.isdir(args.output):
        output = os.path.join(args.output, "synda_queue_stats.json")
    else:
        output = args.output

    process_report_log(args.log_file, output, args.db_stats_file, args.remap_statuses)
