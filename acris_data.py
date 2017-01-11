#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime as dt
import MySQLdb
import warnings
import queue
import threading
import urllib
import requests
import bs4
import os
import argparse
import pycurl
import sys
import log

logger = log.setup_custom_logger('root', '/share2/PythonProjects/acris-download/real_estate.log', 'w')
# in other modules
#import logging
#logger = logging.getLogger('root')

import fake_useragent
ua = fake_useragent.UserAgent(fallback='chrome')

# Use TOR network for all web traffic
import socks
import socket
import stem.process


def db_truncate_tables(dbName, tableNames):
    # Truncate specified tables from DB
    try:
        dbcnx = MySQLdb.connect(user='vkc', password='Db1978!',
                                  host='VKCNAS',
                                  db=dbName)
        dbcsr = dbcnx.cursor() 
        for tn in tableNames:
            dbcsr.execute(r'TRUNCATE TABLE {};'.format(tn))
    except MySQLdb.Error as err:
        logger.error('DB truncate tables failed: {}'.format(str(err)))
        dbcnx.rollback()
    else:
        dbcnx.commit()
        logger.info('DB truncate table successfully')
    finally:
        dbcsr.close()
        dbcnx.close()
    return

def db_drop_tables(dbName, tableNames):
    # Drop specified tables from DB
    try:
        dbcnx = MySQLdb.connect(user='vkc', password='Db1978!',
                                  host='VKCNAS',
                                  db=dbName)
        dbcsr = dbcnx.cursor() 
        #with warnings.catch_warnings():
        #    warnings.simplefilter('ignore')
        #    dbcsr.excute('DROP TABLE')
        for tn in tableNames:
            dbcsr.execute(r'DROP TABLE IF EXISTS {};'.format(tn))
    except MySQLdb.Warning as warning:
        pass
    except MySQLdb.Error as err:
        logger.error('DB drop tables failed: {}'.format(str(err)))
        dbcnx.rollback()
    else:
        dbcnx.commit()
        logger.info('DB tables dropped successfully')
    finally:
        dbcsr.close()
        dbcnx.close()
    return

def db_execute_SQL(dbName, sql):
    # Execute SQL statement
    #sql = ''
    #for line in open(sqlFilePath, 'r'):
    #    sql += line
    try:
        logger.info('Executing SQL "{}...".'.format(sql[:40]))
        dbcnx = MySQLdb.connect(user='vkc', password='Db1978!',
                                  host='VKCNAS',
                                  db=dbName)
        dbcnx.autocommit(True)
        dbcsr = dbcnx.cursor()
        dbcsr.execute(sql)
    except MySQLdb.Error as err:
        logger.error('SQL "{}..." execution failed: {}'.format(sql[:40], str(err)))
    else:
        logger.info('SQL "{}..." executed sucessfully.'.format(sql[:40]))
    finally:
        dbcsr.close()
        dbcnx.close()
    return

def db_load_table(dbName, tableName, datafilePath):
    # Load csv file into DB
#    print('[{}] Begin loading table {} into database {}'.format(dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S'), tableName, db))
    logger.info('Loading data into {}'.format(tableName))
    try:
        dbcnx = MySQLdb.connect(user='vkc', password='Db1978!', host='VKCNAS', db=dbName)
        dbcnx.autocommit(True)
        dbcsr = dbcnx.cursor()
        dbcsr.execute('LOCK TABLES {} WRITE;'.format(tableName))

        # Truncate table first
        dbcsr.execute('DELETE FROM {};'.format(tableName))
        dbcsr.execute('ALTER TABLE {} auto_increment=1;'.format(tableName))
        # Load data into table
        dbcsr.execute('SET GLOBAL local_infile = "ON";')
        dbcsr.execute('SET UNIQUE_CHECKS = 0')    

        dbcsr.execute('ALTER TABLE {} DISABLE KEYS'.format(tableName))
        dbcsr.execute(r'LOAD DATA LOCAL INFILE "{}" IGNORE INTO TABLE {} CHARACTER SET UTF8MB4 FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY "\"" LINES TERMINATED BY "\n" IGNORE 1 LINES;'.format(datafilePath, tableName))
        dbcsr.execute('ALTER TABLE {} ENABLE KEYS'.format(tableName))
        dbcsr.execute('SET UNIQUE_CHECKS = 1')
        dbcsr.execute('UNLOCK TABLES;')
    except MySQLdb.Error as err:
        logger.error('Failed loading data into {}: {}'.format(tableName, str(err)))
 
    finally:
        dbcsr.close()
        dbcnx.close()
        logger.info('Finished loading data into {}'.format(tableName))
#    print('[{}] Finished loading table {} into database {}'.format(dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S'), tableName, db))
    return


def load_tables_queue(q):
    # Queue for threads to load data into DB
    while not q.empty(): 
        (dbName, tableName, datafilePath, dlURL, luURL) = q.get()

        # Download dataset from web and load into DB if newer data available
        webLastUpdated = web_check_NYCOpenData_last_updated(luURL)
        webLastUpdated = dt.datetime.combine(webLastUpdated, dt.time(0,0,0,0))
        dbLastUpdated = db_check_table_last_updated(dbName, tableName)

        if dbLastUpdated < webLastUpdated:
            web_download_file(dlURL, datafilePath, False, 28800)  
            db_load_table(dbName, tableName, datafilePath)
            db_write_table_last_updated(dbName, tableName, webLastUpdated)
            # Remove downloaded file after loaded into DB
            #try:
            #    os.remove(datafilePath)
            #except OSError:
            #    pass      
            
        q.task_done()
    return

def index_tables_queue(q):
    # Queue for threads to execute SQL file line by line    
    while not q.empty(): 
        (dbName, sql) = q.get()
        db_execute_SQL(dbName, sql)
        q.task_done()
    return

def multithread_tasks(data, numThreads, fn):
    # Multithread process for given tasks (fn with data)
    workQueue = queue.Queue()

    # Enqueue tasks
    for d in data:
        workQueue.put(d)

    # Create new threads
    for i in range(numThreads):
        thread = threading.Thread(target=fn, args=(workQueue,))
        thread.setDaemon(True)
        thread.start()

    # Hold until all threads completed   
    workQueue.join()
    return

def db_remove_last_updated(dbName, tableNames):
    # Remove last updated date on DB tables
    try:
        dbcnx = MySQLdb.connect(user='vkc', password='Db1978!',
                                  host='VKCNAS',
                                  db=dbName)
        dbcsr = dbcnx.cursor()
        sql = r'DELETE FROM table_last_updated WHERE tbl_name IN ({});'.format(','.join(['%s'] * len(tableNames)))
        dbcsr.execute(sql, tableNames)
        logger.info('LastUpdated status sucessfully removed on DB tables {}'.format(','.join(tableNames)))
    except MySQLdb.Error as err:
        logger.error('Failed removing LastUpdated status on DB tables {}: {}'.format(','.join(tableNames), str(err)))
    finally:
        dbcsr.close()
        dbcnx.close()
    return

# ALTER TABLE tableLastUpdated ADD UNIQUE (tableName)
def db_write_table_last_updated(dbName, tableName, lastUpdated=dt.datetime(1900,1,1,0,0,0,0)):
    # Write last updated date on DB table
    try:
        dbcnx = MySQLdb.connect(user='vkc', password='Db1978!',
                                  host='VKCNAS',
                                  db=dbName)
        dbcsr = dbcnx.cursor() 
        #dbcsr.execute(r'INSERT INTO table_last_updated (tbl_name, last_updated) VALUES ("{0:}", "{1:}") ON DUPLICATE KEY UPDATE tbl_name=VALUES("{0:}"), last_updated=VALUES("{1:}");'.format(tableName, lastUpdated.strftime('%Y-%m-%d %H:%M:%S')))
        dbcsr.execute(r'REPLACE INTO table_last_updated (tbl_name, last_updated) VALUES ("{0:}", "{1:}");'.format(tableName, lastUpdated.strftime('%Y-%m-%d %H:%M:%S')))
        logger.info('DB table {} LastUpdated date updated to {}'.format(tableName, lastUpdated.strftime('%Y-%m-%d %H:%M:%S')))
    except MySQLdb.Error as err:
        logger.error('DB table {} LastUpdated date update failed: {}'.format(tableName, str(err)))
    finally:
        dbcsr.close()
        dbcnx.close()
    return

def db_check_table_last_updated(dbName, tableName):
    # Check last updated date on DB table
    lastUpdated = dt.datetime(1900,1,1,0,0,0,0)
    try:
        dbcnx = MySQLdb.connect(user='vkc', password='Db1978!',
                                  host='VKCNAS',
                                  db=dbName)
        dbcsr = dbcnx.cursor() 
        dbcsr.execute(r'SELECT last_updated FROM table_last_updated WHERE tbl_name="{}";'.format(tableName))
        row = dbcsr.fetchone()
        if row: lastUpdated = row[0]
        logger.info('DB table {} last updated on {}'.format(tableName, lastUpdated))
    except MySQLdb.Error as err:
        logger.error('DB table {} LastUpdated date cannot be retrieved: {}'.format(tableName, str(err)))
    finally:
        dbcsr.close()
        dbcnx.close()
    return lastUpdated
    
def web_check_NYCOpenData_last_updated(url):
    # Retrieve last update date on NYCOpenData tables
    #logger.debug(url)
    #logger.debug(ua.chrome)
    r = requests.get(url, headers={'User-Agent': ua.random}) # Use random fake browser header
    soup = bs4.BeautifulSoup(r.text, 'lxml')
    lastUpdated = ''
    # Extract last updated date from html tag
    try:
        lastUpdatedStr = soup.find('span', attrs={'class':'aboutUpdateDate'}).find('span', attrs={'class':'dateReplace'}).text
        lastUpdated = dt.datetime.strptime(lastUpdatedStr, '%b %d, %Y').date()
        logger.info('Web dataset {} last updated on {:%Y-%m-%d}'.format(url, lastUpdated))
    except Exception as err:
        logger.error('Web dataset {} last updated check failed: {}'.format(url, str(err)))
    return lastUpdated

def web_download_file(fileURL, filePath, fileCheck, timeOut=28800):
    # Download file from web
    dlFile = True
    if fileCheck:
        if os.path.exists(filePath):
            dlFile = False
        else:
            dlFile = True

    if dlFile:
        fp = open(filePath, "wb")
        curl = pycurl.Curl()
        curl.setopt(pycurl.URL, fileURL)
        curl.setopt(pycurl.USERAGENT, ua.random) # Use random fake browser header
        #curl.setopt(pycurl.NOPROGRESS, 0)
        #curl.setopt(pycurl.PROGRESSFUNCTION, downloadProgress)
        curl.setopt(pycurl.FOLLOWLOCATION, 1)
        curl.setopt(pycurl.MAXREDIRS, 5)
        curl.setopt(pycurl.CONNECTTIMEOUT, 50)
        curl.setopt(pycurl.TIMEOUT, timeOut)
        curl.setopt(pycurl.FTP_RESPONSE_TIMEOUT, 600)
        curl.setopt(pycurl.NOSIGNAL, 1)
        curl.setopt(pycurl.WRITEDATA, fp)
        try:
            logger.info('Downloading file {} ({})'.format(fileURL, filePath))
            curl.perform()
            logger.info('Downloaded file {}. File size: {:,.0f} bytes, Download speed: {:,.0f} bytes/second, Total-time: {:,.1f}s'.format(filePath, curl.getinfo(curl.SIZE_DOWNLOAD), curl.getinfo(curl.SPEED_DOWNLOAD), curl.getinfo(curl.TOTAL_TIME)))
        except Exception as err: #curl.error as err
            logger.error('Failed downloading file {} ({}): {}'.format(fileURL, filePath, str(err)))
            #import traceback
            #traceback.print_exc(file=sys.stderr)
            #sys.stderr.flush()
        curl.close()
        fp.close()
    return

def refresh_acris_db(reinit, index):
    # Refresh ACRIS database
    logger.info('ACRIS database update process begin.')

    NYCOpenDataAPI = 'https://data.cityofnewyork.us/api/views/'
    NYCOpenDataURL = 'https://data.cityofnewyork.us/d/'
    DB = 'real_estate'
    acrisSchemaSQL = 'acris_schema.sql'
    acrisIndexSQL = 'acris_index.sql'
    acrisSQLPath = r'/share2/PythonProjects/acris-download/'
    acrisFilePath = r'/share2/Web/acris/'
    
    acrisDataSets = { \
    'real_property_legals'     : '8h5j-fqxa',
    'real_property_master'     : 'bnx9-e6tj',
    'real_property_parties'    : '636b-3b5g',
    'real_property_references' : 'pwkr-dpni',
    'real_property_remarks'    : '9p4w-7npp',

    'personal_property_legals'     : 'uqqa-hym2',
    'personal_property_master'     : 'sv7x-dduq',
    'personal_property_parties'    : 'nbbg-wtuz',
    'personal_property_references' : '6y3e-jcrc',
    'personal_property_remarks'    : 'fuzi-5ks9',

    'document_control_codes' : '2bix-e6dg',
    'pp_ucc_collateral_codes': 'ww8k-ffcr',
    'property_type_codes'    : 'skeq-tph2',
    'state_codes'            : 'c4dn-m2c2',
    'country_codes'          : 'g3xg-ym2u'
    }
    
    #ACRIS-Subterranean-Rights/56kn-pnny
    #ACRIS-Air-Rights/cxkz-3jrg
    
    # Assemble URLs for file download and last updated check
    dlURLparts = urllib.parse.urlsplit(NYCOpenDataAPI)
    dlQuery = {'accessType': 'DOWNLOAD'}
    luURLparts = urllib.parse.urlsplit(NYCOpenDataURL)
    acrisDBTableFileURLList = [(DB, \
                             'acris_'+key, \
                             os.path.join(acrisFilePath, 'acris_'+key+'.csv'),\
                             urllib.parse.urlunsplit((dlURLparts.scheme, dlURLparts.netloc, dlURLparts.path + val + '/rows.csv', urllib.parse.urlencode(dlQuery), dlURLparts.fragment)),\
                             urllib.parse.urlunsplit((luURLparts.scheme, luURLparts.netloc, luURLparts.path + val, luURLparts.query, luURLparts.fragment))\
                            ) for (key, val) in acrisDataSets.items()]

    # Reinitialize DB if requested
    if reinit:
        db_drop_tables(DB, ['acris_'+key for key in acrisDataSets.keys()])
        db_execute_SQL(DB, open(os.path.join(acrisSQLPath, acrisSchemaSQL),'r').read())
        db_remove_last_updated(DB, ['acris_'+key for key in acrisDataSets.keys()])
        #db_truncate_tables(DB, ['table_last_updated'])

    # Use Tor network for downloading/scraping
    #SOCKS_PORT=9050
    #tor_process = stem.process.launch_tor_with_config(
    #    config = {
    #        'SocksPort': str(SOCKS_PORT),
    #    },
    #)
    #socks.setdefaultproxy(proxy_type=socks.PROXY_TYPE_SOCKS5, addr="127.0.0.1", port=SOCKS_PORT) #sets default proxy which all further socksocket objects will use, unless explicitly changed.
    #socket.socket = socks.socksocket # returns a socket object which is assigned to socket.socket which opens a socket. Now all connections made by the script will be done using this socket.

    # Update DB tables if more updated data available on Web
    multithread_tasks(acrisDBTableFileURLList, 5, load_tables_queue)
    
    # Create indices on DB tables if requested
    if index:
        try:
            # Read SQL file line by line and enqueue them
            logger.info('Re-indexing ACRIS database tables.')
            sql = []
            for line in open(os.path.join(acrisSQLPath, acrisIndexSQL), 'r'):
                sql.append((DB, line))
            multithread_tasks(sql, 5, index_tables_queue)
        except Exception as err:
            logger.error('Re-indexing ACRIS database tables failed.')
        finally:
            logger.info('Re-indexing ACRIS database tables completed.')
        
    logger.info('ACRIS database update process completed.')

    #tor_process.kill()
    return

#def warning_on_one_line(message, category, filename, lineno, file=None, line=None):
#    return ' {}:{}: {}:{}{}'.format(filename, lineno, category.__name__, message, data)

def main():
    # Parse parameters
    argParser = argparse.ArgumentParser()
    argParser.add_argument('-r', '--reinit', help='Re-initialize all database tables and re-import all data', action='store_true')
    argParser.add_argument('-i', '--index', help='Create indices on all database tables', action='store_true')
    #argParser.add_argument('-ll', '--loglevel', default='info', choices=['debug', 'info', 'warning', 'error'], help='logging level (defaults to "info")')
    #argParser.add_argument('-lf', '--logfile', help='Log file location', type=str, default='real_esate.log')
    args = argParser.parse_args()
    #logging.basicConfig(filename=args.logfile, level=getattr(logging, args.log.upper(), None), format='[%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # Force warnings.warn() to omit the source code line in the message
    formatwarning_orig = warnings.formatwarning
    warnings.formatwarning = lambda message, category, filename, lineno, line=None: \
                                formatwarning_orig(message, category, filename, lineno, line='')     
    warnings.showwarning = lambda message, category, filename, lineno, file=None, line=None: \
                                sys.stdout.write(warnings.formatwarning(message, category, filename, lineno))
    warnings.filterwarnings("ignore", "Unknown table") # Ignore unknown table warnings when dropping table

    refresh_acris_db(args.reinit, args.index)
    return

if __name__ == '__main__':
    main()