
import os
import json
import sqlite3

def get_synda_queue_info(db):
    conn = sqlite3.connect(db)

    cmd = "SELECT status, COUNT(*), SUM(size) FROM file GROUP BY status"
    curs = conn.cursor()
    curs.execute(cmd)
    results = curs.fetchall()

    conn.commit()
    conn.close()

    return results

def main():

    with open('db_backups_list.json') as f:
        db_files = json.load(f)

    db_status_history = {}
    for timestamp,db_path in db_files.items():
        if not os.path.isfile(db_path):
            print(f"{db_path} is not a file. Skipping.")
            continue

        try:
            db_status_info = get_synda_queue_info(db_path)
        except Exception as err:
            print(f"Could not read {db_path}: {err}")
            continue

        db_status_dict = {s[0]:dict(file_count=s[1],size=s[2]) for s in db_status_info}

        db_status_history[timestamp] = db_status_dict

        db_status_filename = os.path.basename(db_path) + '.json'
        outpath = 'db_status_files'
        if not os.path.exists(outpath):
            os.makedirs(outpath)
        db_status_filepath = os.path.join(outpath, db_status_filename)
        with open(db_status_filepath, 'w') as db_status_file:
            db_status_file.write(json.dumps({timestamp: db_status_dict}, indent=4))

if __name__ == '__main__':
    main()
