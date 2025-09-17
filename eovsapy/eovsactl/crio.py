#from ftplib import FTP
import glob
from .util import Time, ant_str2list
import os
import ftplib, urllib.request, urllib.error, urllib.parse

def reload_crio_ini(ant_str=None, folder="/home/sched/Dropbox/PythonCode/Current/crio_inis/"):
    '''This function takes a standard string of antennnas and finds the
    newest version of the ini file for each crio and loads them via ftp to
    those crios. An example antenna string is as follows:
    
    "ant1-5 ant7 ant9-12"
    
    The function returns the number of crios that were successfully updated.
    Note that the function searches the eovsapy.crio
    /home/sched/Dropbox/PythonCode/Current/crio_inis/
    for the most recent crio file. The files must have the format of
    
    crio##-yyyy-mm-dd.ini
    
    where ## is a one or two digit number denoting the crio, yyyy is 
    the year, mm is the month and dd is the day of the ini file.
    '''
    
    if ant_str is None:
        print("Error: No antenna list given.")
        return 0
    
    ants = ant_str2list(ant_str)
    crio = ['crio'+str(i+1) for i in ants]
    cwd = os.getcwd()
    os.chdir(folder)
    ncrio=0
    for c in crio:
        files = glob.glob(c + "-????-??-??.ini")
        if len(files) == 0:
            print("No files found for", c)
        else:
            t = []
            for f in files:
                t.append(Time(f[-14:-4], out_subfmt = 'date'))
					
            session = ftplib.FTP(c + '.solar.pvt','admin','observer')
            session.cwd("ni-rt/startup")
            f = open(files[t.index(max(t))], 'r')
            session.storbinary("STOR crio.ini", f)
            f.close()
            session.close()
            ncrio += 1
            print("ini file written to", c)
    
    os.chdir(cwd)
    print("crio.ini files written to", ncrio, "CRIOs")
    return ncrio

def save_crio_ini(ant_str=None, folder="/home/sched/Dropbox/PythonCode/Current/crio_inis/"):
    ''' Copies crio.ini files from crios to a standard location appended with
        the current date.  These files can be transferred back to the crio in
        case the crio file gets corrupted.
    '''
    if ant_str is None:
        print("Error: No antenna list given.")
        return 0
    
    ants = ant_str2list(ant_str)
    crio = ['crio'+str(i+1) for i in ants]
	
    userpass = 'admin:observer@'
    cwd = os.getcwd()
    os.chdir(folder)
    datstr = Time.now().iso[:10]
    for c in crio:
        # Get crio.ini lines
        f = urllib.request.urlopen('ftp://'+userpass+c+'.solar.pvt/ni-rt/startup/crio.ini',timeout=0.5)
        lines = f.readlines()
        f.close()
        # Convert from bytes to strings for Python3
        lines = [line.decode('UTF-8') for line in lines]
        # Write crio.ini lines to file
        f = open(c+'-'+datstr+'.ini','w')
        for line in lines: 
            f.write(line)
        f.close()
    os.chdir(cwd)
    
    print('crio.ini files copied from',crio)
