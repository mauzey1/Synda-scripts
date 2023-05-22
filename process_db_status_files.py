###############################################################
# Script for processing the files that contain file counts and 
# storage sizes for each status in Synda's database backup files.
# It is similar to the script used to process weekly reports log.
###############################################################

import os
import sys
import glob
import json
import argparse

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

def process_db_status_files(db_status_dir, output, remap_statuses=False):

    db_status_files = glob.glob(os.path.join(db_status_dir,"sdt.db*.json"))
                                
    if db_status_files is not None:
        log_dict = {}
        for f in db_status_files:
            db_status = json.load(open(f,'r'))
            for timestamp, status_info in db_status.items():

                statuses = {}
                for s,v in status_info.items():
                    if remap_statuses:
                        status = status_remap(s)
                    else:
                        status = s
                    file_count = int(v['file_count'])
                    size = v['size']
                    if status in statuses:
                        statuses[status]['file_count'] += file_count
                        statuses[status]['size'] += size
                    else:
                        statuses[status] = dict(file_count=file_count, size=size)
                for k,v in statuses.items():
                    statuses[k]['size'] = bytes_to_human_read(v['size'])

            log_dict[timestamp] = statuses

            log_dict_sorted = {k: v for k, v in sorted(log_dict.items(), key=lambda item: item[0])}

        with open(output, 'w') as stats_file:
            stats_file.write(json.dumps(log_dict_sorted, indent=4))

if __name__ == '__main__':

    p = argparse.ArgumentParser(
        description="Gather the output of 'synda queue' within the CMIP6 replication's report log" )
    p.add_argument( "--remap_statuses", "-rs", required=False, 
                   action="store_true",
                   help="Remap statuses by combining related statuses into one.  This will also combine the file counts and storage sizes of these statuses." )
    p.add_argument( "--db_status_dir", "-d", required=False,
                    help="Directory containing database status files",
                    default="db_status_files" )
    p.add_argument( "--output", "-o", required=False,
                    help="Path of output JSON file",
                    default="db_status_stats.json" )

    args = p.parse_args( sys.argv[1:] )

    if not os.path.isdir(args.db_status_dir):
        print("%s is not a file"%(args.db_status_dir))

    if os.path.isdir(args.output):
        output = os.path.join(args.output, "db_status_stats.json")
    else:
        output = args.output

    process_db_status_files(args.db_status_dir, output, args.remap_statuses)
