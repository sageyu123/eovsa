#
# SQL2MySQL Module
#   Routines for filling, updating, or synchronizing the MySQL databases
#   at OVRO and in the Cloud to keep them up to date with the main MS SQL
#   database at OVRO.
# 
# Written 2022-May-21  DG
#
import eovsapy.dbutil as db
from eovsapy.util import Time

def get_table_columns(table):
    from importlib_resources import files
    tablefile = str(files('eovsapy').joinpath('SourceCat/mysql_tables.txt'))
    columns = []
    with open(tablefile,'r') as f:
        while(1):
            # Read line by line until the CREATE TABLE line for the requested table is found
            line = f.readline()
            if not line:
                # EOF (shouldn't happen unless the table name is not found)
                break
            elif line.find(table+'`') != -1:
                # This is the start of the table
                while(1):
                    # Read line by line until the PRIMARY line is found, signifying the end of the table
                    line = f.readline()
                    if not line:
                        # EOF (shouldn't happen unless the file is corrupt)
                        break
                    elif line.find('PRIMARY') != -1:
                        # This is the PRIMARY line, so return
                        return columns
                    else:
                        # Find the column name for this line and append to the list
                        col = line.split('`')[1]
                        # Workaround for bug in odbc driver that truncates column names to 30 characters
                        if   col == 'Ante_Cont_AzimuthPositionCorre':
                            columns.append('Ante_Cont_AzimuthPositionCorrected')
                        elif col == 'Ante_Cont_ElevationPositionCor':
                            columns.append('Ante_Cont_ElevationPositionCorrected')
                        else:
                            columns.append(col)
    return columns
                    
def fill_mysql_table(table, trange, ocolumns, host=None):
    ''' This routine is called from sqltable2mysql().
    
        Transfers all records of the specified table from MS SQL to MySQL in
        the current timerange.  If the table name is 'abin', then the trange
        is interpreted at the minimum Id of abin records in MySQL and all
        records greater than that Id are transferred from MS SQL to MySQL.
        
        Inputs:
          table      A string giving the table name (one starting 'fV', 'hV', or else 'abin')
          trange     The time object giving the timestamps of records to transfer.  If a
                       timerange, the start and stop times are used.  If a single time,
                       it is used as the start time and the end time is taken as the
                       current time.  NB: If the table name is 'abin', then trange
                       should be a single integer giving the Id of the last abin record in
                       MySQL.  All records in MS SQL with greater Id values are transferred.
          ocolumns   The list of output column names to write to MySQL (returned by
                       get_table_columns()).
          host       The MySQL host name.  If omitted or None, 'localhost' is used.
    '''
    from copy import copy
    columns = copy(ocolumns)
    scnxn, scursor = db.get_cursor('sqlserver.solar.pvt')
    if table != 'abin':
        try:
            # Try trange as a range
            tstart, tend = trange.lv.astype(int)
        except:
            # If a single time, use current time as end time
            tstart = trange.lv.astype(int)
            tend = Time.now().lv.astype(int)
        
    if table == 'abin':
        idmin = str(trange)
        get_query = 'select '+','.join(columns)+' from '+table+' where Id > '+idmin
        get_query = 'set textsize 2147483647 ' + get_query
    else:
        get_query = 'select '+','.join(columns)+' from '+table+' where Timestamp between '+str(tstart)+' and '+str(tend)
    data, msg = db.do_query(scursor, get_query)
    scnxn.close()
    if msg != 'Success':
        print('Error reading from MS SQL')
        print(get_query)
        return msg
    # Make a list of lists
    dlist = []
    for k in data.keys():
        if type(data[k][0]) is str:
            data[k] = [i.strip('\0') for i in data[k]]
        dlist.append(data[k])
    # Transpose the list
    tlist = list(zip(*dlist))
    if host is None:
        mcnxn, mcursor = db.get_cursor('localhost')
    else:
        mcnxn, mcursor = db.get_cursor(host)
    # Return column names to truncated form, if any
    for i,column in enumerate(columns):
        if len(column) > 30: columns[i] = columns[i][:30]
    put_query = 'insert ignore into '+table+' ('+','.join(columns)+') values ('+('%s,'*len(columns))[:-1]+')'
    for i in range(0,len(tlist),100):
        # Break list into at most 100 records at a time
        mcursor.executemany(put_query, tlist[i:i+100])
        mcnxn.commit()
        if i % 1000 == 0:
            if i != 0: print('\r'+'#'*(i//1000),end=' ')
        
    mcnxn.close()
    return msg
    
def sqltable2mysql(table, trange=None, host=None):
    ''' Sends subset of MS SQL table columns to MySQL host specified
        by the host string, for the given timerange.
        
        Inputs:
          table    A string giving the table name to transfer
          trange   A Time() object giving the start and end time of
                     data to transfer
          host     The host name of the MySQL host to update.
                     defaults to 'localhost' if None.
    ''' 
    from numpy import unique, arange
    columns = get_table_columns(table)
    if table[0] == 'f':
        # This is a stateframe table, so select only needed data and 
        # break time into daily chunks
        if trange is None:
            print('Error: The timerange cannot be None')
            return
        import eovsapy.dump_tsys as dt
        mjd0 = trange[0].mjd
        mjd1 = trange[1].mjd
        # If the timerange ends exactly at 00:00:00, assume the user did not
        # mean to add the next day.
        if (mjd1 % 1) == 0.0:
            mjd1 -= 1
        for mjd in arange(mjd0, mjd1+1):
            fdb = dt.rd_fdb(Time(mjd,format='mjd'))
            if fdb != {}:
                scans, idx = unique(fdb['SCANID'],return_index=True)
                idx.sort()
                idx = idx.tolist()
                if fdb['SCANID'][idx[-1]] != '':
                    # IFDB tables have an extra "blank" scan, so the code below
                    # does not use the last scan.  But if the last scan is not
                    # a blank SCANID, then add a fake last scan.
                    idx += [0]
                nidx = len(idx)
                for i in range(nidx-1):
                    tend = float(fdb['EN_TS'][idx[i+1]-1])
                    tstart = float(fdb['ST_TS'][idx[i]])
                    if tend < trange[0].lv:
                        # This scan ends before requested start time, so skip it
                        pass
                    else:
                        # Make sure start time is requested start time or later
                        tstart = max(trange[0].lv,tstart)
                        # Make sure end time is requested end time or earlier
                        tend = min(trange[1].lv,tend)
                        tran = Time([tstart, tend],format='lv')
                        if tran[0] >= trange[1]:
                            print('\nAll Done!')
                            return
                        msg = fill_mysql_table(table, tran, columns, host=host)
                        print('\r',tran[1].iso,msg)
    elif table[0] == 'h':
        # This is a scanheader table, so we will do 30 days at a time
        if trange is None:
            print('Error: The timerange cannot be None')
            return
        mjd0 = trange[0].mjd
        mjd1 = trange[1].mjd
        for mjd in arange(mjd0, mjd1, 30):
            tran = Time([mjd,min(mjd+30,mjd1)],format='mjd')
            msg = fill_mysql_table(table, tran, columns, host=host)
            print('\r',tran[1].iso,msg)
    else:
        # This is the abin table, so ignore the trange and transfer all new
        # records from MS SQL to MySQL
        if trange: 
            print('Error: No timerange should be given for abin records')
        else:
            print('All MS SQL abin records newer than the last MySQL abin record will be transferred.')
        mcnxn, mcursor = db.get_cursor(host=host)
        get_query = 'select Id,Timestamp from abin order by Id desc limit 1'
        data, msg = db.do_query(mcursor, get_query)
        if msg != 'Success':
            print('Error reading from MySQL')
            print(get_query)
            return msg
        idmin = data['Id'][0]
        mcnxn.close()
        msg = fill_mysql_table(table, idmin, columns, host=host)

    print('\nAll Done!')
    return
                
def sync2mysql(test=False):
    ''' Attempt to synchronize the fV* and hV* tables from MS SQL to the two MySQL databases 
        (MySQL at OVRO, MySQL in cloud). This reads the top time from the MS SQL tables and
        compares with the top times of the MySQL tables, and if the latter ends sooner the
        resulting timerange of new records are sent for each of four tables.
        
        Inputs:
          test   Optional--if True, no records are transferred but it prints out what it
                   would have done.
    '''
    from time import time
    # First attempt to connect to each database
    cnxn1, cursor1 = db.get_cursor('sqlserver.solar.pvt')
    if cnxn1 is None:
        print('Error: Cannot connect to MS SQL database.  Cannot continue.')
        return 'Error: Cannot connect to MS SQL database.'
    cnxn2, cursor2 = db.get_cursor('localhost')
    cnxn3, cursor3 = db.get_cursor('amazonaws.com')
    if cnxn2 is None and cnxn3 is None:
        print('Error: Cannot connect to either MySQL database.  Cannot continue.')
        return 'Error: Cannot connect to either MySQL database.'
    cnx = [cnxn2, cnxn3]
    curs = [cursor2, cursor3]

    for i in range(2):
        cnxn = cnx[i]
        cursor = curs[i]
        host = cnxn.server_host
        if host[:9] == 'eovsa-db0': host = 'amazonaws.com'
        # The transfer has to be done for each of the four tables fV66_vD1, fV66_vD15, hV37_vD1, hV37_vD50
        tbls = ['hV37_vD1', 'hV37_vD50', 'fV66_vD1', 'fV66_vD15']
        for tbl in tbls:
            # Initial check of timestamps:
            query = 'select top 1 Timestamp from '+tbl+' order by Timestamp desc'
            data, msg = db.do_query(cursor1, query)
            if msg == 'Success':
                SQLtime = data['Timestamp'][0]
                columns = get_table_columns(tbl)
            else:
                print('Error reading SQL time for table '+tbl,msg)
                SQLtime = None
            if SQLtime:
                query = 'select Timestamp from '+tbl+' order by Timestamp desc limit 1'
                data, msg = db.do_query(cursor, query)
                if msg == 'Success':
                    MySQLtime = data['Timestamp'][0]
                else:
                    print('Error reading MySQL time from',host,'for table'+tbl,msg)
                    MySQLtime = SQLtime
                if MySQLtime < SQLtime:
                    trange = Time([MySQLtime,SQLtime],format='lv')
                    if test:
                        print('Would have updated',tbl,trange.iso,'on host',host)
                    else:
                        t0 = time()
                        print('Updating',tbl,trange.iso,'on host',host)
                        sqltable2mysql(tbl, trange, host=host)
                        print('Updated',tbl,trange.iso,'on host',host+': Took',time()-t0,'s')
    cnxn1.close()
    if cnxn2: cnxn2.close()
    if cnxn3: cnxn3.close()

def abin2all3(timestamp, version, description, buf):
    ''' Attempt to send a single abin record to all three databases 
        (MS SQL at OVRO, MySQL at OVRO, MySQL in cloud).
        The arguments are carefully checked to ensure a valid abin record
        is written.
    '''
    from pyodbc import Binary
    msg = ''
    # Verify inputs are reasonable
    try:
        t = Time(timestamp,format='lv')
    except:
        print('Error: Could not interpret timestamp as a LabVIEW time.')
        return 'Error: Could not interpret timestamp as a LabVIEW time.'
    try:
        if version < 1.0 or version > 20.0:
            print('Error: Version is not in the expected range 1-20')
            return 'Error: Version is not in the expected range 1-20'
    except:
        print('Error: Could not interpret Version as a number.')
        return 'Error: Could not interpret Version as a number.'
    import eovsapy.cal_header as ch
    typdict = ch.cal_types()
    good = False
    for i in range(1,len(typdict)+1):
        if description == typdict[i][0]:
            good = True
            break
    if not good:
        print('Error: Description is not one of the known descriptions.')
        return 'Error: Description is not one of the known descriptions.'
    try:
        cnxn, cursor = db.get_cursor('sqlserver.solar.pvt')
        cnxn.cursor().execute('insert into abin (Timestamp,Version,Description,Bin) values (?, ?, ?, ?)',
                    timestamp, version, description, Binary(buf))
        cnxn.cursor().commit()
        cnxn.close()
    except:
        msg += 'Error: Could not send abin record to MS SQL; '
        print('Error: Could not send abin record to MS SQL; ')
    try:
        cnxn, cursor = db.get_cursor('localhost')
        cnxn.cursor().execute('insert into abin (Timestamp,Version,Description,Bin) values (%s, %s, %s, %s)',
                             (timestamp, version, description, buf))
        cnxn.commit()
        cnxn.close()
    except:
        msg += 'Error: Could not send abin record to OVRO SQL; '
        print('Error: Could not send abin record to OVRO MySQL')
    try:
        cnxn, cursor = db.get_cursor('amazonaws.com')
        cnxn.cursor().execute('insert into abin (Timestamp,Version,Description,Bin) values (%s, %s, %s, %s)',
                             (timestamp, version, description, buf))
        cnxn.commit()
        cnxn.close()
    except:
        msg += 'Error: Could not send abin record to Cloud SQL; '
        print('Error: Could not send abin record to Cloud MySQL')
    if msg == '':
        msg = 'Success'
    return msg