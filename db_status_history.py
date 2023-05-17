
import os
import json
import sqlite3
import argparse
import multiprocessing

def get_synda_queue_info(db):
    conn = sqlite3.connect(db)

    cmd = "SELECT status, COUNT(*), SUM(size) FROM file GROUP BY status"
    curs = conn.cursor()
    curs.execute(cmd)
    results = curs.fetchall()

    conn.commit()
    conn.close()

    return results

def process_db_file(timestamp, db_path, outpath):

    if not os.path.isfile(db_path):
        print(f"{db_path} is not a file. Skipping.")
        return

    try:
        db_status_info = get_synda_queue_info(db_path)
    except Exception as err:
        print(f"Could not read {db_path}: {err}")
        return

    db_status_dict = {s[0]:dict(file_count=s[1],size=s[2]) for s in db_status_info}

    db_status_filename = os.path.basename(db_path) + '.json'
    db_status_filepath = os.path.join(outpath, db_status_filename)
    with open(db_status_filepath, 'w') as db_status_file:
        db_status_file.write(json.dumps({timestamp: db_status_dict}, indent=4))

def main():

    parser = argparse.ArgumentParser(
        description="Gather file count and storage size per Synda status in backup database files. \
                    This script will generate one JSON file of file status stats per database file.")
    parser.add_argument("--db_list_file", "-f", dest="db_list_file", type=str,
                        default=None, help="JSON file containing a list of database backup file paths and timestamps of their creation")
    parser.add_argument("--output", "-o", dest="output", type=str,
                        default=os.path.curdir, help="Output directory for stat files (default is current directory)")
    parser.add_argument("--num_procs", "-np", dest="num_procs", type=int, default=1,
                        help="Number of processes to run (default is 1)")
    args = parser.parse_args()

    if args.db_list_file is None:
        print("You must input a JSON file containing a list of 'timestamp':'db_file_path' pairs")
        return
    elif not os.path.isfile(args.db_list_file):
        print(f"{args.db_list_file} is not a file. Exiting.")
        return

    if not os.path.isdir(args.output):
        print(f"{args.output} is not a directory. Exiting.")
        return

    num_processes = 1
    if args.num_procs > 1:
        num_processes = args.num_procs

    with open(args.db_list_file) as f:
        db_files_dict = json.load(f)
    db_files_list = list(db_files_dict.items())

    db_files_list_with_outdir = [i + (args.output,) for i in db_files_list]

    with multiprocessing.Pool(num_processes) as pool:
        pool.starmap(process_db_file, db_files_list_with_outdir)


if __name__ == '__main__':
    main()
