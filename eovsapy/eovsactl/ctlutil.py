#
#  ctlutil
#
#  Utility functions for control of EOVSA
#

def send_cmds(cmds,acc):
    ''' Sends a series of commands to ACC.  The sequence of commands
        is not checked for validity!
        
        cmds   a list of strings, each of which must be a valid command
        acc    the ACC dict returned by rd_ACCfile().
    '''
    import socket

    for cmd in cmds:
        #print 'Command:',cmd
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((acc['host'],acc['scdport']))
            s.send(cmd)
            time.sleep(0.01)
            s.close()
        except:
            print('Error: Could not send command',cmd,' to ACC.')
    return
