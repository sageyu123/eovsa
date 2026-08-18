#!/usr/bin/env python
#
# History:
#   2016-Jan-16  DG
#     Started this history log.  Added ant_toggle() routine to toggle the
#     power of various devices in the field at each antenna.
#   2016-Jan-19  DG
#     Added Queue use to ant_toggle so that calling program (schedule.py)
#     can receive messages when spawned by threading module.
#   2016-Jan-22  DG
#     Changed the __main__ code to accept a couple of command-line args.
#   2016-Jan-27  DG
#     In an attempt to make ant_toggle() more reliable, it tries to login
#     three times before each of the two toggle commands.
#   2026-Jul-20  SY
#     Added state-aware read-toggle-verify control for frontend power.

import requests
from requests.auth import HTTPDigestAuth
import telnetlib, time
import Queue
import threading
import xml.etree.ElementTree as ET

q = Queue.Queue()
_power_locks = {}
_power_locks_guard = threading.Lock()
_request_timeout = 5

def _get_power_lock(antnum):
    '''Return the process-local lock for one Viking antenna controller.'''
    with _power_locks_guard:
        if antnum not in _power_locks:
            _power_locks[antnum] = threading.Lock()
        return _power_locks[antnum]

def _get_relay_state(antnum, relay):
    '''Read and return a Viking electrical relay state as 0 or 1.'''
    url = 'http://vik%d.solar.pvt/protect/status.xml' % antnum
    response = requests.get(url, auth=HTTPDigestAuth('admin','pwr4me'),
                            timeout=_request_timeout)
    try:
        if response.status_code != 200:
            raise IOError('HTTP status %s from %s' %
                          (response.status_code, url))
        value = ET.fromstring(response.content).findtext('.//rly%d' % relay)
    finally:
        response.close()
    if value is None or value.strip() not in ('0', '1'):
        raise ValueError('Invalid relay %d state: %r' % (relay, value))
    return int(value)

def _toggle_relay(antnum, relay):
    '''Issue exactly one toggle request to a Viking electrical relay.'''
    url = ('http://vik%d.solar.pvt/protect/'
           'relays.cgi?relay=%d&state=toggle' % (antnum, relay))
    response = requests.get(url, auth=HTTPDigestAuth('admin','pwr4me'),
                            timeout=_request_timeout)
    try:
        if response.status_code != 200:
            raise IOError('HTTP status %s from %s' %
                          (response.status_code, url))
    finally:
        response.close()

def ant_set_power(antnum, device='fem', power_on=True):
    '''Ensure that an antenna frontend is in the requested power state.

    :param antnum: Antenna number whose Viking relay controller is used.
    :type antnum: int
    :param device: ``fem`` or ``frontend`` (relay 2).
    :type device: str
    :param power_on: A Boolean specifying the requested FEM device state.
    :type power_on: bool
    :returns: ``True`` only when the requested state is verified.
    :rtype: bool

    Relay 2 uses the normally-closed contact, so the electrical relay state
    reported by the Viking controller is the inverse of FEM device power.
    The controller only offers a toggle operation; this routine therefore
    holds a lock across a read, at most one toggle, and a verification read.
    It never retries an ambiguous toggle.
    '''
    if not isinstance(power_on, bool):
        q.put('Ant%d FEM power error: requested state must be Boolean' % antnum)
        return False
    try:
        device_name = device.upper()
    except AttributeError:
        device_name = ''
    if device_name not in ('FEM', 'FRONTEND'):
        q.put('Ant%d power error: unsupported device %s' %
              (antnum, str(device)))
        return False

    # Normally-closed wiring inversion:
    # relay OFF (0) means FEM ON; relay ON (1) means FEM OFF.
    target_relay_state = 0 if power_on else 1
    requested_state = 'ON' if power_on else 'OFF'

    with _get_power_lock(antnum):
        try:
            relay_state = _get_relay_state(antnum, 2)
        except Exception as err:
            q.put('Ant%d FEM power %s failed: cannot read Frontend relay 2: %s' %
                  (antnum, requested_state, str(err)))
            return False

        if relay_state == target_relay_state:
            q.put('Ant%d FEM power already %s (Frontend relay 2 is %s)' %
                  (antnum, requested_state,
                   'OFF' if relay_state == 0 else 'ON'))
            return True

        toggle_error = None
        try:
            _toggle_relay(antnum, 2)
        except Exception as err:
            # The request may have reached the controller before its response
            # failed.  Verify once, but never issue a second blind toggle.
            toggle_error = err
            q.put('Ant%d FEM power %s toggle response failed; verifying state: %s' %
                  (antnum, requested_state, str(err)))
        time.sleep(0.5)
        try:
            relay_state = _get_relay_state(antnum, 2)
        except Exception as err:
            q.put('Ant%d FEM power %s failed: cannot verify Frontend relay 2: %s' %
                  (antnum, requested_state, str(err)))
            return False

        if relay_state == target_relay_state:
            suffix = ' after an ambiguous toggle response' if toggle_error else ''
            q.put('Ant%d FEM power verified %s (Frontend relay 2 is %s)%s' %
                  (antnum, requested_state,
                   'OFF' if relay_state == 0 else 'ON', suffix))
            return True

        q.put('Ant%d FEM power %s failed verification: Frontend relay 2 is %s' %
              (antnum, requested_state,
               'OFF' if relay_state == 0 else 'ON'))
        return False

def ant_toggle(antnum, device=None, wait=None, cycle=True):
    '''Run a legacy Viking power cycle without interleaving a power-set call.

    :param antnum: Antenna number whose Viking relay controller is used.
    :type antnum: int
    :param device: Device name, or ``None`` for the antenna controller.
    :type device: str or None
    :param wait: Seconds to leave the device off; defaults to 15.
    :type wait: int or None
    :param cycle: Restore device power after the first toggle when ``True``.
    :type cycle: bool
    :returns: ``None``; status messages are written to :data:`q`.
    :rtype: None
    '''
    with _get_power_lock(antnum):
        return _ant_toggle(antnum, device, wait, cycle)

def _ant_toggle(antnum, device=None, wait=None, cycle=True):
    ''' Toggles power to one of the devices attached to the Viking Relay switch
        in each antenna controller box.  If cycle=True, then end state is ALWAYS
        for the device to be turned ON (relay turned OFF).
        
        The possible devices are 'antenna' or 'ant', 'frontend' or 'fem', 'crio'.
        There is also a relay 4 that can be switched by specifying 'other',
        although this will not affect anything unless a hardware change is made.
        These names are not case sensitive.
        
        The requested device is powered off for 15 s, then back on, unless
        wait is set to a number of seconds to wait.
        
        All messages go into Queue q, which can be read by the calling program.
    '''
    relaydef = {'ANTENNA':1, 'ANT':1, 'FEM':2, 'FRONTEND':2, 'CRIO':3, 'OTHER':4}
    devstr = {1: 'Antenna', 2: 'Frontend', 3: 'CRIO', 4: 'Relay 4'}
    if device is None:
        # Default to cycling the antenna controller
        relay = 1
    else:
        try:
            relay = relaydef[device.upper()]
        except:
            q.put('Ant'+str(antnum)+' Error interpreting device '+device)
            return

    url = 'http://vik'+str(antnum)+'.solar.pvt/protect/'+'relays.cgi?relay='+str(relay)+'&state=toggle'
    dur = 15  # Default duration to wait
    if wait:
        if type(wait) != int:
            q.put('Ant'+str(antnum)+' Warning: Could not interpret wait duration',wait,'.  Must be an integer type.  Will use 15 s')
            dur = 15
        else:
            dur = wait
            
    # See if we can connect and toggle the relay
    try:
        stat_code = vik_login(antnum)
        if stat_code != 200:
            return
            
        # Actually send the request to toggle the relay
        r = requests.get(url,auth=HTTPDigestAuth('admin','pwr4me'))
        
        # Write the result of the request to the output queue.
        #    The if statement fixes the peculiarity that the relay is off 
        #    when power is on and vice versa.  The "result" gives the state 
        #    of the power, not the relay
        if r.text[-3:] == 'off':
            q.put('Ant'+str(antnum)+' '+r.text.replace('off','on'))
        else:
            q.put('Ant'+str(antnum)+' '+r.text.replace('on','off'))

        # If we are supposed to cycle the power, send the request again after a wait
        if cycle == True:
            if r.text == devstr[relay]+' now on':
                # Request to turn relay on (to turn device off) worked,
                # so wait for requested length of time and then toggle
                # again to turn device back on.
                time.sleep(dur)
                stat_code = vik_login(antnum)
                if stat_code != 200:
                    return

                # Actually send the request to toggle the relay
                r = requests.get(url,auth=HTTPDigestAuth('admin','pwr4me'))

                # Write the result of the request to the output queue.
                if r.text[-3:] == 'off':
                    q.put('Ant'+str(antnum)+' '+r.text.replace('off','on'))
                else:
                    q.put('Ant'+str(antnum)+' '+r.text.replace('on','off'))
        # Close the connection
        r.close()
        return
    except:
        q.put('Ant'+str(antnum)+' Error communicating with Viking Relay '+devstr)
        return

def vik_login(antnum):
    ''' Try 3 times to log in with authentication information
    '''
    url = 'http://vik'+str(antnum)+'.solar.pvt/protect/'
    # Try to log in 3 times, with 1 s delay between each
    for i in range(3):
        try:
            q.put('Ant'+str(antnum)+' attempt #'+str(i+1)+' to login.')
            test = requests.get(url,auth=HTTPDigestAuth('admin','pwr4me'))
        except requests.ConnectionError as e:
            q.put('Ant'+str(antnum)+' Error could not connect to '+url+'.  Message: '+e.message.message)
            return None
        if test.status_code == 200:
            q.put('Ant'+str(antnum)+' Login successful.')
            break
        else:
            q.put('Ant'+str(antnum)+' Login failed with status code: '+str(test.status_code))
        time.sleep(1)
    if test.status_code != 200:
        q.put('Ant'+str(antnum)+' Error HTML status code: '+str(test.status_code))
    return test.status_code
        

def pwr_cycle(host,loadn,user='admin',passwd='pwr4me',wait=None):
    ''' Connect to a Tripp Lite PDU (Power Distribution Unit) and power cycle one of the loads.
           host    One of the PDUs, one of 'pdunetwork.solar.pvt', 'pduanalog.solar.pvt', or 'pdudigital.solar.pvt'
           user    Username, currently set to 'admin' for all three PDUs
           passwd  Password
           loadn   Integer load number to power cycle
           wait    Duration [s] to wait before turning load back on 
                     if omitted, "Cycle" is used instead of "Off" followed by "On
        Returns True if successful, False otherwise
    '''
    if wait is None:
        #           Username, Password,    Devices, Device, Actions, Loads, Load #,         Cycle, X,    X,    X,    Logout
        term_str = ['login:', 'Password:', '> ',    '> ',   '> ',    '> ',  '> ',           '> ',  '> ', '> ', '> ', '> ']
        response = [user+'\n',passwd+'\n', '1\n',   '1\n',  '2\n',   '2\n', str(loadn)+'\n','3\n', 'X\n','X\n','X\n','X\n']
        dur = False
    else:
        #           Username, Password,    Devices, Device, Actions, Loads, Load #,         Off,   On,   X,    X,    X,    Logout
        term_str = ['login:', 'Password:', '> ',    '> ',   '> ',    '> ',  '> ',           '> ',  '> ', '> ', '> ', '> ', '> ']
        response = [user+'\n',passwd+'\n', '1\n',   '1\n',  '2\n',   '2\n', str(loadn)+'\n','2\n', '2\n','X\n','X\n','X\n','X\n']
        if type(wait) != int:
            print 'Warning: Could not interpret wait duration',wait,'.  Must be an integer type.  Will use 30 s'
            dur = 30
        else:
            dur = wait

    # Initiate connection
    tn = telnetlib.Telnet(host)
    time.sleep(1)

    # Loop over response
    for i in range(len(term_str)):
        out = tn.read_until(term_str[i],1)
        if out[-len(term_str[i]):] != term_str[i]:
            print 'Telnet connection to',host,'timed out.'
            tn.close()
            return False
            break
        if dur and i == 8:
            # Wait dur [s] before turning load back on
            time.sleep(dur)
        tn.write(response[i])
    tn.close()
    return True

def pwr_off(host,user,passwd,loadn):
    ''' Connect to a Tripp Lite PDU (Power Distribution Unit) and power down one of the loads.
           host    One of the PDUs, one of 'pdunetwork.solar.pvt', 'pduanalog.solar.pvt', or 'pdudigital.solar.pvt'
           user    Username, currently set to 'admin' for all three PDUs
           passwd  Password
           loadn   Integer load number to power down
        Returns True if successful, False otherwise
    '''
    #           Username, Password,    Devices, Device, Actions, Loads, Load #,         Off,   X,    X,    X,    Logout
    term_str = ['login:', 'Password:', '> ',    '> ',   '> ',    '> ',  '> ',           '> ',  '> ', '> ', '> ', '> ']
    response = [user+'\n',passwd+'\n', '1\n',   '1\n',  '2\n',   '2\n', str(loadn)+'\n','2\n', 'X\n','X\n','X\n','X\n']

    # Initiate connection
    tn = telnetlib.Telnet(host)
    time.sleep(1)

    # Loop over response
    for i in range(len(term_str)):
        out = tn.read_until(term_str[i],1)
        if out[-len(term_str[i]):] != term_str[i]:
            print 'Telnet connection to',host,'timed out.'
            tn.close()
            return False
            break
        tn.write(response[i])
    tn.close()
    return True

if __name__ == "__main__":
    import sys
    host = 'pdudigital.solar.pvt'
    port = 14
    if len(sys.argv) == 3:
        host = sys.argv[1]
        port = sys.argv[2]
    pwr_cycle(host,port)
