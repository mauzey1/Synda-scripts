#!/usr/bin/env python

"""For every retracted file or dataset, change its  status in the Synda database to one containing
the string 'retracted'.  For example,  'error' or 'error*'  (where * is a wildcard) to
'error-retracted' and 'done' to 'done-retracted'.
An input file lists the retracted datasets.  You can generate such a file from the JSON file at
web pages like this one (for CNRM CMIP6 data):
  'https://esgf-node.llnl.gov/esg-search/search?data_node=esg1.umr-cnrm.fr&format=application%2fsolr%2bjson&retracted=true&mip_era=CMIP6&limit=10000'
and extracting the lines containing '"id":'.

This can also change the status with other suffixes; just provide your preferred suffix as a
second argument, after the filename.
And a list of files can be provided rather than a list of datasets. If so, the parameters should
be 'files', filename, [suffix]
"""

import sys, pdb
import logging
import sqlite3
import debug
global conn, Nupdates, Nchanges
conn = None

def setup():
    """Initializes logging and the connection to the database, etc."""
    global conn, Nupdates

    logfile = '/p/css03/scratch/logs/status_retracted.log'
    logging.basicConfig( filename=logfile, level=logging.INFO, format='%(asctime)s %(message)s' )

    Nupdates = 0

    # normal:
    if conn is None:
        timeout = 12000  # in seconds; i.e. 200 minutes
        conn = sqlite3.connect( '/var/lib/synda/sdt/sdt.db', timeout )
        # test on a temporary copy of the database:
        #conn = sqlite3.connect( '/home/mauzey1/db/sdt.db', timeout )

def finish():
    """Closes connections to databases, etc."""
    global conn
    conn.commit()
    conn.close()
    conn = None

def list_data_nodes():
    """returns a list of data_nodes in the database"""
    global conn
    setup()
    try:
        cmd = "SELECT data_node FROM file GROUP BY data_node ORDER BY COUNT(*)"
        curs = conn.cursor()
        curs.execute( cmd )
        datanodes = curs.fetchall()
    except Exception as e:
        logging.debug("status_retracted.list_data_nodes() saw an exception %s" %e )
        raise e
    finally:
        curs.close()
    return datanodes

def file_retracted_status( file_id, suffix='retracted' ):
    """Updates the status of a single file which has been retracted.  file_id can be a number
    or a 1-tuple containg a number, which is a file_id in the Synda database."""
    global conn, Nupdates
    try:    # convert tuple to a number
        file_id=file_id[0]
    except:
        pass
    try:
        cmd = "SELECT status FROM file WHERE file_id=%s" % file_id
        curs = conn.cursor()
        curs.execute( cmd )
        results = curs.fetchall()  # e.g. [('done',)]
    except Exception as e:
        logging.debug("status_retracted.file_retracted_status() 1 saw an exception %s" %e )
        raise e
    finally:
        curs.close()
    assert( len(results) == 1 )
    status = results[0][0]    # e.g. 'done'
    if status[-len(suffix):]==suffix:
        return
    elif status=='running':
        # This is the most likely of the cases where another process wants to change the status too.
        raise Exception("running file, try again later")
    elif status[0:4]=='done':
        newstatus = 'done,'+suffix
    elif status[0:5]=='error' or status=='waiting' or status=='obsolete' or status[0]=='_':
        # All of these are data we don't have.  A status beginning with '_' is a temporary
        # status set manually.
        newstatus = suffix
    elif status[0:9]=='published':
        newstatus = 'published,'+suffix
    else:
        newstatus = status+','+suffix
    if status==newstatus:
        #print "From file_id", file_id, "results=", results, "status=",status
        return
    #print "From file_id", file_id, "results=", results, "status=",status, "newstatus=",newstatus
    try:
        cmd = "UPDATE file SET status='%s' WHERE file_id=%s" % (newstatus,file_id)
        curs = conn.cursor()
        curs.execute( cmd )
    except Exception as e:
        logging.debug("status_retracted.file_retracted_status() 2 saw an exception %s" %e )
        raise e
    finally:
        curs.close()
    Nupdates += 1
    #logging.info( "file %s changed from %s to %s " % (file_id,status,new_status) )

def dataset_retracted_status( dataset_fid, suffix='retracted' ):
    """
    updates the status of files in a single dataset which has been retracted.  Also updates the
    status of the dataset.  The input  argument is its dataset_functional_id in the Synda database.
    If the dataset isn't in the database, the database is not changed.
    """
    global conn, Nupdates, Nchanges
    cmd = "SELECT file_id FROM file WHERE dataset_id IN "+\
          "(SELECT dataset_id FROM dataset WHERE dataset_functional_id='%s')" % dataset_fid
    try:
        curs = conn.cursor()
        curs.execute( cmd )
        fresults = curs.fetchall()
    except Exception as e:
        logging.debug("status_retracted.dataset_retracted_status() 1 saw an exception %s" %e )
        raise e
    finally:
        curs.close()
    if len(fresults)==0:
        # We usually get datasets into the database before they are retracted and disappear.
        # Do we have this dataset at all?  Is it superceded by a later version?
        vers = dataset_fid[-9:]
        same_version_exists = False
        newer_version_exists = False
        #cmd = "SELECT dataset_functional_id FROM dataset WHERE dataset_functional_id LIKE '%s%%'"\
        #      % ( dataset_fid[:-9], )
        cmd = "SELECT path_without_version FROM dataset WHERE dataset_functional_id='%s'" %\
              dataset_fid
        try:
            curs = conn.cursor()
            curs.execute(cmd)
            presults = curs.fetchall()
        except Exception as e:
            logging.debug("status_retracted.dataset_retracted_status() 2 saw an exception %s" %e)
            raise e
        finally:
            curs.close()
        if len(presults)==0:
            # The dataset is not in the database.
            return
        else:
            # The dataset is in the database.
            assert( len(presults)==1 ) 
            path_without_version = presults[0]
            cmd = "SELECT dataset_functional_id FROM dataset WHERE path_without_version='%s'" %\
                  path_without_version
            try:
                curs = conn.cursor()
                curs.execute(cmd)
                dresults = curs.fetchall()
            except Exception as e:
                logging.debug("status_retracted.dataset_retracted_status() 3 saw an exception %s"
                              %e)
                raise e
            finally:
                curs.close()
            for dfid in dresults:
                if dfid[0]==dataset_fid:
                    same_version_exists = True
                if dfid[0][-9:]>vers:
                    newer_version_exists = True
            if same_version_exists and not newer_version_exists:
                logging.warning( "Dataset %s is retracted but there is no newer version!"
                                 % dataset_fid )
    #print "From dataset_fid",dataset_fid, "results=",fresults

    # Change the statuses of component files
    try:
        for file_id in fresults:
            file_retracted_status( file_id, suffix )
    except Exception as e:
        # Whatever went wrong, it may not happen again, e.g. trying to to change the status of
        # a 'running' file.  Leave the dataset unchanged for now, and re-raise the exception so that
        # this will be noticed.  Cross our fingers and hope it will work if we try again later.
        logging.error("couldn't finish changing the status of dataset %s" % dataset_fid)
        raise e

    # Finally change the status of this dataset itself
    cmd = "SELECT status FROM dataset WHERE dataset_functional_id='%s'" % dataset_fid
    try:
        curs = conn.cursor()
        curs.execute( cmd )
        results = curs.fetchall()  # e.g. [('complete',)]
    except Exception as e:
        logging.debug("status_retracted.dataset_retracted_status() 4 saw an exception %s" %e)
        raise e
    finally:
        curs.close()
    assert( len(results) <= 1 )
    if len(results)==1:
        status = results[0][0]    # e.g. 'complete'
        if status.find(suffix)<0:
            new_status = status + ','+suffix
            cmd = "UPDATE dataset SET status='%s' WHERE dataset_functional_id='%s'" %\
                  (new_status,dataset_fid)
            try:
                curs = conn.cursor()
                curs.execute( cmd )
            except Exception as e:
                logging.debug("status_retracted.dataset_retracted_status() 5 saw an exception %s" %e)
                raise e
            finally:
                curs.close()
            logging.info( "dataset %s changed from %s to %s " % (dataset_fid,status,new_status) )
            Nchanges += 1

    if Nupdates>=100:
        conn.commit()
        Nupdates = 0

def status_retracted( datasets, suffix='retracted' ):
    """Input is the path of a text file which contains a list of retracted datasets, as
    dataset_functional_ids or other forms which we can parse.  For each file, belonging to one of
    these datasets, and for which its status in the Synda database is appropriate, its
    status will be changed to 'retracted' (if we don't have it) or 'published-retracted' or
    'done-retracted' if we have it.  The dataset status will be changed similarly.
    (If supplied, another suffix will be used in place of 'retracted').
    """
    global Nchanges
    Nchanges = 0
    setup()
    logging.info( "Reading list of retracted datasets " + datasets )
    with open( datasets, 'r' ) as f:
        for line in f:
            # If this line comes from a JSON file, these operations will be likely to get just
            # the dataset:
            dataset_fid = line.strip()
            if dataset_fid[0:24] == '<str name="instance_id">':
                dataset_fid = dataset_fid[24:]        
            elif dataset_fid[0:15] == '<str name="id">':
                dataset_fid = dataset_fid[15:]        
            elif dataset_fid[0:5] == '"id":':
                dataset_fid = dataset_fid[5:]
            elif dataset_fid[0] == '"':
                dataset_fid = dataset_fid[1:]
            elif dataset_fid[-2:] == '",':
                dataset_fid = dataset_fid[:-2]
            if dataset_fid.find('|')>0:
                # if the line has 'id' and came from a JSON file, it ends with | followed
                # by the data node
                dataset_fid = dataset_fid.split('|')[0] # deletes '|' and everything after.
            dataset_fid = dataset_fid.replace('</str>','')
            try:
                dataset_retracted_status( dataset_fid, suffix )
            except Exception as e:
                logging.error( "status_retracted() saw an exception %s" %e )
                raise e
    finish()
    logging.info( "Finished processing retracted datasets " + datasets )
    logging.info( "%s datasets were newly marked as %s" % (Nchanges,suffix) )
    return Nchanges

def files_retracted( files, suffix='retracted' ):
    """Like status_retracted, but changes the status of only files.  Input is a list of
    filenames; we do no parsing or cleaning of anything else.
    ESGF retraction is supposed to be done by dataset, not file; but sometimes
    this can be useful.
    """
    setup()
    with open( files, 'r' ) as f:
        for line in f:
            filename = line.strip()
            cmd = "SELECT file_id FROM file WHERE filename='%s'" % filename
            try:
                curs = conn.cursor()
                curs.execute( cmd )
                results = curs.fetchall()
            except Exception as e:
                logging.debug("status_retracted.files_retracted() saw an exception %s" %e)
                raise e
            finally:
                curs.close()
            assert( len(results)==1 )
            file_id = results[0][0]
            file_retracted_status( file_id, suffix )
    finish()

if __name__ == '__main__':
    suffix = 'retracted'
    if len( sys.argv ) > 1:
        if sys.argv[1]=='file':
            if len( sys.argv ) > 3:
                suffix = sys.argv[3]
            # sys.argv[2] should be or the name of a file listing retracted files
            files_retracted( sys.argv[2], suffix )
        else:
            if len( sys.argv ) > 2:
                suffix = sys.argv[2]
            # sys.argv[1] should be or the name of a file listing retracted datasets
            status_retracted( sys.argv[1], suffix )
    else:
        print "please provide an input file, containing a list of retracted datasets"
        print "Or start with the keyword 'file', and continue with the name of a file"+\
            "containing a list of retracted files"
        print "Optionally, you can supply a final argument, the suffix to be added to"+\
            " status names."
