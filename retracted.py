#!/usr/bin/env python

"""Finds retracted datasets, writes a list.
Simplest usage:
  retracted.py
This will get the names of up to 20,000 retracted datasets from the index node.
It will start at an offset read from a file, and write a new offset to the file:
the old offset plus the number of datasets discovered in this run. """

import sys, re, datetime
import subprocess as sp
import argparse, logging, time
import debug, pdb
import status_retracted

class numFoundException(Exception):
    """numFound was too big"""
    pass

def get_starting_offset():
    """Returns the starting offset for queries for retracted datasets.
    We are assuming that the index node always returns retracted datasets in the same
    order, so that starting where we left off before will get us all the new ones,
    and only the new ones.  (This assumption may not always be true, necessitating
    an occasional restart from offset=0.)
    """
    foffset = '/p/css03/scratch/publishing/CMIP6_Nretracted'
    try:
        with open(foffset,'r') as f:
            starting_offset = f.readline().strip()
        return int( starting_offset )
    except IOError as e:
        logging.error( "Cannot open CMIP6_Nretracted "+e )
        return 0

def save_starting_offset( starting_offset ):
    """Saves the starting offset for queries for retracted datasets.  See get_starting_offset."""
    foffset = '/p/css03/scratch/publishing/CMIP6_Nretracted'
    with open(foffset,'w') as f:
        f.write( str(starting_offset) )

def retract_path(path, test):
    """Retract datasets listed in the path text file retrieved by one_query.
    If this is a test of the query system, and the database is not to be referenced.
    Returns the number of datasets which were newly marked as retracted.
    """
    # Record the retracted datasets in the database:
    Nchanges = 0
    if not test:
        try:
            Nchanges = status_retracted.status_retracted( path+'.txt' )
            # ... this defaults to suffix='retracted'
        except Exception as e:
            # database access errors are what I want to be prepared for, but I'm
            # catching all exceptions here
            logging.error("Failed with exception %s" % e.__repr__() )
            logging.error("We can try again some day.")
            #   ... For AssertionError, e or str(e) prints as ''.
            # But an error here usually is a "database is locked".  Rather than do the right
            # thing, I'll do the simplest to code:  wait 10 minutes and go on.  Maybe we'll
            # succeed the next time status_retracted is called on this data.
            # The '-1' is a flag to tell the caller to leave starting_offset unchanged so the
            # next run will retry from the same point.
            time.sleep(600)
            return 0
    return Nchanges

def one_query( cmd, path ):
    """Does one query specified by cmd.
    Returns the number of datasets received in the response, which can be used
    to compute the next offset.  Also returns numFound, extracted from the response.
    The other arguments are a file path for the json input file and txt output file (without
    the '.json' and '.txt' suffixes)."""

    logging.info( "cmd=%s" % cmd )
    wget_out = "undefined wget_out"
    try:
        wget_out = str(sp.check_output(cmd, shell=True, stderr=sp.STDOUT))
        logging.info( "wget_out=%s" % wget_out )
    except Exception as e:
        logging.error( "one_query, wget exception, wget_out=%s" % wget_out )
        logging.error( " exception is %s" % e )
        try:
            logging.error( "return code=%s" % e.returncode )
        except:
            pass
        try:
            logging.error( "error output=%s" % e.output )
        except:
            pass
        raise e

    # logging and convert json output to a text list of datasets:
    cmd = "grep numFound "+path+".json"
    # Apply the grep command.
    # BTW this is simpler but prints the whole numFound line: sp.call(cmd, shell=True)
    # example of nFstr:
    #  '<result name="response" numFound="132311" start="0" maxScore="1.0">\n'
    nFstr = str(sp.Popen(cmd, shell=True, stdout=sp.PIPE).stdout.read())
    # example of nF:  'numFound="132311" start="0" '
    nF = nFstr[nFstr.find('numFound'):nFstr.find('maxScore')]
    # example of numFound (an int):  132311
    numFound = list(map(int, re.findall(r'\d+',nF) ))[0]
    logging.info( nF )
    cmd = 'grep \\"instance_id\\" '+path+'.json > '+path+'.txt'
    sp.call(cmd, shell=True)
    with open(path+'.txt') as fids: num_lines = len(fids.readlines())
    # ... a terse way to count lines, memory hog ok because file is <10K lines.
    if num_lines<numFound:
        logging.warning( "one_query numFound=%s>num_lines=%s from %s !" % (numFound,num_lines,path) )

    logging.info( "num_lines=%s" % num_lines )

    return num_lines, numFound

def get_retracted( prefix,
                   starting_offset=0, npages=20, test=False ):
    """Queries the index node for a list of retracted datasets, starting at the prescribed
    offset in the list, and continuing for the prescribed number of 10,000-dataset pages.
    Writes the results to files whose names begin with the specified prefix.
    If test be True, then the next ostarting offset will not be saved for future runs.
    The numFound returned is the maximum over all pages (if nothing had changed at the server while
    this function is running, then all pages' values of numFound would be the same).
    Also returns Nchanges, the number of datasets which were newly marked as retracted.
    """
    numFoundmax = 0
    cmd = "wget -O "+prefix+"%s.json 'https://esgf-node.llnl.gov/esg-search/search?project=CMIP6&retracted=true&fields=instance_id&replica=false&limit=10000&offset=%s'"\
          % (starting_offset,starting_offset)
    Nchangesall = 0
    for N in range(npages):
        path = prefix+str(starting_offset)
        num_lines, numFound = one_query( cmd, path )
        numFoundmax = max( numFoundmax, numFound )
        if num_lines==0:
            # No more datasets to be found
            break
        try:
            Nchanges = retract_path(path, test)
        except Exception as e:
            # Flag to retry
            continue
        Nchangesall += Nchanges
        starting_offset += num_lines
        logging.info( "next_offset=%s" % starting_offset )
        if not test:
            save_starting_offset( starting_offset )
    return numFoundmax, Nchangesall

def get_some_retracted( prefix, constraints='', test=True ):
    """Like get_retracted, but the query is limited as specified and _not_ paginated.
    The string constraints is the concatenation of 0 or more constraints separated by '&'.
    Example of a constraint: "data_node=esgf-data3.ceda.ac.uk".
    Two numbers are returned:  numFound and Nchanges, the number of datasets which were newly
    marked as retracted.
    """
    constr2 = constraints.replace('!=','=NOT')
    constr3 = '_'.join([con.split('=')[1] for con in constr2.split('&') if con.find('=')>=0])
    path = prefix + constr3
    cmd = "wget -O "+path+'.json'+\
          " 'https://esgf-node.llnl.gov/esg-search/search?project=CMIP6&retracted=true&" +\
          constraints +\
          "&fields=instance_id&replica=false&limit=10000'"
    num_lines, numFound = one_query( cmd, path )
    logging.info( "get_some_retracted; constraints=%s, num_lines=%s, numFound=%s"%
                  (constraints, num_lines, numFound ) )
    if num_lines<numFound:
        logging.warning( "get_some_retracted numFound=%s>num_lines=%s !" % (numFound,num_lines) )
        raise numFoundException
    else:
        Nchanges = retract_path(path, test)
        return numFound, Nchanges

def get_some_retracted_paginated( prefix, constraints='', test=True ):
    """Like get_some_retracted, but the query _is_ paginated.
    """
    constr2 = constraints.replace('!=','=NOT')
    constr3 = '_'.join([con.split('=')[1] for con in constr2.split('&') if con.find('=')>=0])
    numFoundMax = 0
    Nchangesall = 0
    starting_offset = 0
    page = 0
    while True:
        path = prefix + constr3 + '.' + str(page)
        cmd = "wget -O "+path+'.json'+\
            " 'https://esgf-node.llnl.gov/esg-search/search?project=CMIP6&retracted=true&" +\
            constraints +\
            "&fields=instance_id&replica=false&limit=10000&offset=%s'"%(starting_offset)
        num_lines, numFound = one_query( cmd, path )
        logging.info( "get_some_retracted_paginated; constraints=%s, num_lines=%s, numFound=%s"%
                    (constraints, num_lines, numFound ) )
        numFoundMax = max( numFoundMax, numFound )
        if num_lines==0:
            # No more datasets to be found
            break
        try:
            Nchanges = retract_path(path, test)
        except Exception as e:
            # Flag to retry
            continue
        Nchangesall += Nchanges
        starting_offset += num_lines
        if starting_offset >= numFoundMax:
            # Last page reached
            break
        page += 1
    
    return numFoundMax, Nchangesall

def get_retracted_facet( prefix, facet, facets, constraint='', test=True ):
    """Like get_retracted, but rather than do a paginated query this queries separately
    for each in a list of facets.  For example, facet='frequency' would have
    facets=['1hr', '1hrCM', 'day', 'mon',...].
    An additionl constraint may be supplied.
    Returns  the sum of numFound returned from all queries issued.
    Also returns Nchanges, the number of datasets which were newly marked as retracted.
    N.B. get_retracted_multi_facets is usually a better choice"""
    numFoundall = 0
    Nchangesall = 0
    for fct in facets:
        constraints = facet+'='+fct
        if constraint!='':
            constraints += '&'+constraint
        numFoundnow, Nchangesnow = get_some_retracted( prefix, constraints, test )
        numFoundall += numFoundnow
        Nchangesall += Nchangesnow
    constraints = '&'.join([ facet+'!=%s'%fct for fct in facets ])
    if constraint!='':
        constraints += '&'+constraint
    numFoundnow, Nchangesnow = get_some_retracted( prefix, constraints, test )
    numFoundall += numFoundnow
    Nchangesall += Nchangesnow
    return numFoundall, Nchangesall

def get_retracted_multi_facets( prefix, fcts, constraints='', complement_query=True,\
                                test=True ):
    """Like get_retracted, but rather than do a paginated query, queries separately for
    each combination of facets supplied by fcts.  fcts is a list of pairs (2-tuples),
    (fct, fcts1) where fct is a facet name, e.g. 'data_node' and fcts is a list of
    its possible values, e.g. ["esgf_data3.ceda.ac.uk","esg.lasg.ac.cn"].  In this example,
    a query will be issued with the constraint 'data_node=esgf_data3.ceda.ac.uk' and another
    with the constraint 'data_node=esg.lasg.ac.cn'.
    If the optional argument complement_query=True is supplied, then another query will be
    issued for the complement of the unionof all the facets supplied, e.g.
    'data_node!=esgf_data3.ceda.ac.uk&data_node!=esg.lasg.ac.cn'.
    If fcts=[], no queries will be issued.  If len(fcts)>1, then all possible combinations
    will be issued.  For example, with input
    [('data_node',['esgf_data3.ceda.ac.uk','esg.lasg.ac.cn'],
     ('frequency',['1hr','3hr']) ], complement_query=False
    four queries will be issued containing these constraints
    'data_node=esgf_data3.ceda.ac.uk'&frequency=1hr'
    'data_node=esgf_data3.ceda.ac.uk'&frequency=3hr'
    'data_node=esg.lasg.ac.cn&frequency=1hr'
    'data_node=esg.lasg.ac.cn&frequency=3hr'

    Returns  the sum of numFound returned from all queries issued successfully.  That is,
    queries which did not have to be redone as several queries, each with more constraints.
    Also returns Nchanges, the number of datasets which were newly marked as retracted.
    """
    if len(fcts)==0 and len(constraints)==0:
        return
    elif len(constraints)>0:
        try:
            # Maybe the constraints are already enough to keep the query results small enough.
            if len(fcts)>0:
                return get_some_retracted( prefix, constraints, test )
            else:
                return get_some_retracted_paginated( prefix, constraints, test )
        except numFoundException as e:
            # There are too many query results; we have to constrain another facet.
            if len(fcts)>0:
                pass
            else:
                raise e
    facet = fcts[0][0]
    facets = fcts[0][1]
    numFoundall = 0
    Nchangesall = 0
    for fct in facets:
        fct_constraint = facet+'='+fct
        if fct_constraint != '':
            fct_constraints = '&'.join( [fct_constraint, constraints] )
            numFound, Nchangesnow = get_retracted_multi_facets(
                prefix, fcts[1:], fct_constraints, complement_query, test )
            numFoundall += numFound
            Nchangesall += Nchangesnow
    if complement_query:
        fct_constraint = '&'.join([ facet+'!=%s'%fct for fct in facets ])
        if fct_constraint != '':
            fct_constraints = '&'.join( [fct_constraint, constraints] )
            numFound, Nchangesnow = get_retracted_multi_facets(
                prefix, fcts[1:], fct_constraints, complement_query, test )
            numFoundall += numFound
            Nchangesall += Nchangesnow
    return numFoundall, Nchangesall

def my_data_nodes():
    """returns a list of data nodes."""
    # For now this is hardwired, because access to my test database in ~/db/ is slow.
    # In the future, for the working databse in /var/lib/synda/, I may use my function
    # status_retracted.list_data_nodes().
    return [
        "esg-dn1.nsc.liu.se", "esgf-data3.ceda.ac.uk", "esgf.bsc.es", "esgf3.dkrz.de",
        "esgf-data2.diasjp.net", "noresg.nird.sigma2.no", "esgf-data.ucar.edu",
        "esg-dn2.nsc.liu.se", "crd-esgf-drc.ec.gc.ca", "esgf.ichec.ie", "esgf-cnr.hpc.cineca.it",
        "esg.lasg.ac.cn", "vesg.ipsl.upmc.fr", "esg1.umr-cnrm.fr", "esgf.nccs.nasa.gov",
        "dpesgf03.nccs.nasa.gov", "esgf.nci.org.au", "cmip.bcc.cma.cn", "esgf-data3.diasjp.net",
        "esgdata.gfdl.noaa.gov", "esgf-node2.cmcc.it", "dist.nmlab.snu.ac.kr", "esgf.dwd.de",
        "esg-cccr.tropmet.res.in", "esgf.rcec.sinica.edu.tw", "cmip.fio.org.cn", "aims3.llnl.gov",
        "cmip.dess.tsinghua.edu.cn", "gridftp.ipsl.upmc.fr", "esgf-nimscmip6.apcc21.org",
        "esg.camscma.cn", "polaris.pknu.ac.kr", "esgf-data-fedcheck.ceda.ac.uk",
        "vesgint-data.ipsl.upmc.fr", "esgf-node.gfdl.noaa.gov", "esgf-data1.llnl.gov",
        "esg-dn1.tropmet.res.in", "esgf-data.csc.fi", "esgf-data2.llnl.gov", "esgf1.dkrz.de" ]

def get_retracted_std3( prefix, complement_query=True, test=True ):
    """Runs get_retracted_multi_facets on my three standard facets: data_node,
    time frequency, and realm.  Complements (i.e. anything not matching)
    are searched for iff complement_query be True.
    Returns numFound.
    Also returns Nchanges, the number of datasets which were newly marked as retracted."""
    # At present the frequency list is complete and the data_node list highly incomplete.
    frequencies = [ '1hr', '1hrCM', '3hr', '3hrPt', '6hr', '6hrPt', 'day', 'dec', 'fx',
                    'mon', 'monC', 'monPt', 'month', 'subhrPt', 'yr', 'yrPt' ]
    # data_nodes = ["esgf-data3.ceda.ac.uk"]
    data_nodes = my_data_nodes()
    realms = [ 'aerosol', 'atmos', 'atmosChem', 'land', 'landIce', 'ocean', 'ocnBgChem',
               'ocnBgchem', 'seaIce' ]
    activities = [ 'AerChemMIP', 'C4MIP', 'CFMIP', 'CMIP', 'DAMIP', 'DCPP', 'HighResMIP', 'LUMIP',
                   'PAMIP', 'RFMIP', 'ScenarioMIP', 'VolMIP' ]
    fcts = [ ('data_node',data_nodes), ('frequency',frequencies), ('realm',realms),
             ('activity_id',activities) ]
    return get_retracted_multi_facets( prefix, fcts, '', complement_query, test)

def get_retracted_frequency( prefix, test=True ):
    """Like get_retracted, but rather than do a paginated query this queries separately
    for each in a list of time frequencies.
    Returns numFound.
    Also returns Nchanges, the number of datasets which were newly marked as retracted."""
    frequencies = [ '1hr', '1hrCM', '3hr', '3hrPt', '6hr', '6hrPt', 'day', 'dec', 'fx',
                    'mon', 'monC', 'monPt', 'month', 'subhrPt', 'yr', 'yrPt' ]
    return get_retracted_facet( prefix, 'frequency', frequencies, '', test )

def get_retracted_data_node( prefix, test=True ):
    """Like get_retracted, but rather than do a paginated query this queries separately
    for each in a list of data_nodes.
    Returns numFound.
    Also returns Nchanges, the number of datasets which were newly marked as retracted."""
    data_nodes = ["esgf-data3.ceda.ac.uk"]
    return get_retracted_facet( prefix, 'data_node', data_nodes, '', test )

def get_retracted_data_node_orig( prefix="/home/syndausr/retracted/some-retracted-", test=True ):
    """Like get_retracted, but rather than do a paginated query this queries separately
    for each in a list of data_nodes.
    Returns numFound.
    Also returns Nchanges, the number of datasets which were newly marked as retracted."""
    data_nodes = ["esgf-data3.ceda.ac.uk"]
    for dn in data_nodes:
        get_some_retracted( prefix, 'data_node='+dn, test )
    constraints = '&'.join([ 'data_node!=%s'%dn for dn in data_nodes ])
    return get_some_retracted( prefix, constraints, test )

if __name__ == '__main__':
    # Set up logging and arguments, then call the appropriate 'run' function.
    logfile = '/p/css03/scratch/logs/retracted.log'
    logging.basicConfig( filename=logfile, level=logging.INFO, format='%(asctime)s %(message)s' )

    p = argparse.ArgumentParser(
        description="Query the LLNL index node for retracted datasets." )
    p.add_argument( "--starting_offset", dest="starting_offset", required=False, default=None )
    p.add_argument( "--prefix", dest="prefix", required=False,
                    default="/home/syndausr/retracted/all-retracted-" )
    p.add_argument( "--npages", dest="npages", required=False, type=int, default=20 )
    p.add_argument('--test', dest='test', action='store_true')
    p.add_argument('--no-test', dest='test', action='store_false')
    p.add_argument('--chunking', dest='chunking', default='std3' )
    p.set_defaults( test=False )

    args = p.parse_args( sys.argv[1:] )

    starting_offset = args.starting_offset
    if starting_offset is None:
        starting_offset = get_starting_offset()
    prefix = args.prefix
    npages = args.npages
    test = args.test
    chunking = args.chunking
    prefix = prefix + str(datetime.datetime.now().day) + '-' # append day of the month

    if chunking=='paginated':  #doesn't work, function is deleted
        numFound, Nchanges = get_retracted_paginated( prefix, starting_offset, npages, test )
    elif chunking=='data_node':
        numFound, Nchanges = get_retracted_data_node( prefix, test )
    elif chunking=='std3':
        numFound, Nchanges = get_retracted_std3( prefix, False, test )
    else:
        print("bad argument --chunking=",chunking,"should be 'paginated' or 'data_node' or 'std3'")
        logging.error(
            "bad argument --chunking=",chunking,"should be 'paginated' or 'data_node' or 'std3'")
    logging.info( "End of retracted.py.  numFound=%s, Nchanges=%s" % (numFound, Nchanges) )
