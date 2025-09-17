# Module
#  accFTP
# Routines for getting and putting files to ACC via FTP
# History:
#   2022-03-28  DG
#     First compiled from other routines in eovsactl package
#

# This uses the .netrc feature for FTP credentials.
# There must be a .netrc file in the home directory of the 
# user with the FTP username and password for the ACC.
# The file contents are as follows, where the credentials
# are given in place of <string>:
#
''' 
machine acc.solar.pvt
        login <login-name>
        password <passwd>
'''
import netrc
HOST = 'acc.solar.pvt'
username, acct, password = netrc.netrc().authenticators(HOST)
userpass = username+':'+password+'@'
from ftplib import FTP

def ACC_DCMtable2dict():
    ''' Returns the contents of the DCM_master_table.txt file stored on the ACC
        as a dictionary with a time object and the [52, 30] array of 
        DCM attenuations.
        
        Returns and empty dict {} on error.
    '''
    try:
        # Log in to ACC and change to parm folder
        acc = FTP(HOST)
        acc.login(username, password)
        acc.cwd('parm')
    except:
        print('ACC_DCMtable2dict: login failed')
        return {}
    lines = []
    try:
        # Retrieve the lines of the file
        result = acc.retrlines('retr DCM_master_table.txt',lines.append)
        # Use last modified date as timestamp
        dtstr = acc.voidcmd('MDTM DCM_master_table.txt')[4:].strip()
        datstr = dtstr[:4]+'-'+dtstr[4:6]+'-'+dtstr[6:8]+' '+dtstr[8:10]+':'+dtstr[10:12]+':'+dtstr[12:]
        t = Time(datstr)
    except:
        print('ACCdlatable2dict: DCM_master_table.txt could not be retrieved')
        return {}
    try:
        # Parse the ascii file to get the data (52 non-comment lines with band + 30 attns)
        bands = np.zeros(52, 'int')
        attn = np.zeros((52, 30), 'int')
        for line in lines:
            if line[0] != '#':
                band, rline = line.strip().split(':')
                attn[int(band) - 1] = list(map(int, rline.split()))
                bands[int(band) - 1] = band
        return {'Time':t, 'DCMattn':attn, 'Bands':bands}
    except:
        print('ACCdlatable2dict: DCM_master_table.txt not of expected format.')
        return ()

def ACCdlatable2dict():
    ''' Returns the contents of the delay_centers.txt file stored on the ACC
        as a dictionary with a time object and the [16, 2] array of delays in ns.
        
        Returns and empty dict {} on error.
    '''
    try:
        # Log in to ACC and change to parm folder
        acc = FTP(HOST)
        acc.login(username, password)
        acc.cwd('parm')
    except:
        print('ACCdlatable2dict: login failed')
        return {}
    lines = []
    try:
        # Retrieve the lines of the file
        result = acc.retrlines('retr delay_centers.txt',lines.append)
    except:
        print('ACCdlatable2dict: delay_centers.txt could not be retrieved')
        return {}
    try:
        # Parse the ascii file to get the data
        
        # Get time from header line 2
        t = Time(lines[1][12:])
        # Read file of delays (16 non-comment lines with ant, dlax, dlay)
        tau_ns = np.zeros((16, 2), 'float')
        for line in lines:
            if line[0] != '#':
                ant, xdla, ydla = line.strip().split()
                tau_ns[int(ant) - 1] = np.array([float(xdla), float(ydla)])
        return {'Time':t, 'Delaycen_ns':tau_ns}
    except:
        print('ACCdlatable2dict: delay_centers.txt not of expected format.')
        return ()

def ACC_stor(filename, destfilename=None, dest='parm', binary=False):
    ''' Transfers the data in an existing file to the ACC via FTP.
        
        Inputs:
           filename     A text string giving the path and filename to send.
           dest         The name of the destination folder.  Currently
                          can be 'parm' or 'ni-rt/startup'.  Default 'parm'
           destfilename The name to use for the file on the ACC.  If None, 
                          the destination filename is taken as the stem of 
                          the input filename
           binary       A boolean signifying if the input file is a binary
                          file.  Default is False, i.e. it is treated as
                          a text file.
    '''
    import os
    if destfilename is None:
        destfilename = os.path.basename(filename)
    acc = FTP(HOST)
    acc.login(username, password)
    acc.cwd(dest)
    if binary:
        f = open(filename, 'rb')
        # Send binary file to ACC
        print(acc.storbinary('STOR '+destfilename, f))
    else:
        f = open(filename, 'r')
        # Send lines to ACC
        print(acc.storlines('STOR '+destfilename, f))
    f.close()
    print('Successfully wrote '+destfilename+' to ACC')

def ACC_get(filename, orig='parm'):
    ''' Returns the contents of a text file FTPd from the ACC.
        A list of lines is returned.
    
        Inputs:
           filename     The name of the file on the ACC.
           orig         The name of the folder to retrieve the file
                          from.  Currently can be 'parm' or 'ni-rt/startup'.
                          Default 'parm'
    '''
    import urllib.request
    f = urllib.request.urlopen('ftp://'+userpass+HOST+'/'+orig+'/'+filename, timeout=1)
    lines = f.readlines()
    f.close()
    return lines