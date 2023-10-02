#!/usr/bin/env python

"""
This script will update the status of datasets that have all of their files
download from 'empty' to 'complete'.

This script was made as a workaround for Synda 3.35, which doesn't update
the status of a dataset once all of its files are downloaded.
"""

import sys
import argparse
import sqlite3
import logging
from datetime import datetime


def mark_completed_datasets(db, dry_run=False):
    timeout = 12000  # in seconds; i.e. 200 minutes
    conn = sqlite3.connect( db, timeout )

    try:
        query = """
                SELECT COUNT(*) FROM dataset WHERE dataset_id IN (
                    SELECT f.dataset_id FROM file AS f
                    INNER JOIN dataset AS d ON f.dataset_id=d.dataset_id
                    WHERE d.crea_date>'2023-07-27' AND d.status='empty'
                    GROUP BY f.dataset_id
                    HAVING COUNT(*) = COUNT(CASE WHEN f.status = 'done' then 1 end)
                );
                """
        curs = conn.cursor()
        curs.execute(query)
        results = curs.fetchone()
        dataset_count = results[0]
        logging.info( f"Found {dataset_count} to be marked 'completed'" )
    except Exception as e:
        logging.error( "Exception in mark_completed_datasets: %s" %e )
        raise e
    finally:
        curs.close()

    if not dry_run:
        try:
            latest_date = datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')

            cmd = """
                    UPDATE dataset SET status='complete',latest_date='{latest_date}' WHERE dataset_id IN (
                        SELECT f.dataset_id FROM file AS f
                        INNER JOIN dataset AS d ON f.dataset_id=d.dataset_id
                        WHERE d.crea_date>'2023-07-27' AND d.status='empty'
                        GROUP BY f.dataset_id
                        HAVING COUNT(*) = COUNT(CASE WHEN f.status = 'done' then 1 end)
                    );
                """.format(latest_date=latest_date)
            curs = conn.cursor()
            curs.execute( cmd )
            conn.commit()
        except Exception as e:
            logging.error( "Exception in mark_completed_datasets: %s" %e )
            raise e
        finally:
            curs.close()

    logging.info( "Done" )
    conn.commit()
    conn.close()


if __name__ == '__main__':
    logfile = '/p/css03/scratch/logs/mark_completed_datasets.log'
    logging.basicConfig( filename=logfile, level=logging.INFO, format='%(asctime)s %(message)s' )

    p = argparse.ArgumentParser(
        description="Change the status of datasets that have all of their files with the status 'done' " \
                    "from 'empty' to 'published' in a Synda database" )

    p.add_argument( "--dryrun", required=False, action="store_true" )
    p.add_argument( "--database", required=False, help="a Synda database",
                    default="/var/lib/synda/sdt/sdt.db" )

    args = p.parse_args( sys.argv[1:] )

    if args.dryrun:
        logging.info( "Starting dry run" )
    else:
        logging.info( "Starting" )

    try:
        mark_completed_datasets(args.database, args.dryrun)
    except:
        sys.exit(1)

