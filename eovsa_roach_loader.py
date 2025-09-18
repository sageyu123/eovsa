
# -*- coding: utf-8 -*-
"""
Sequential ROACH loader for EOVSA
---------------------------------
This script connects to each ROACH board, loads the specified BOF design,
and verifies that the FPGA bitstream has been successfully programmed by
checking the available devices. It retries on failures and handles
different return formats from `listdev()`.

Written for Python 2.7 and the `corr.katcp_wrapper` API.
"""
import time
from corr.katcp_wrapper import FpgaClient

# List of ROACH hostnames. Adjust if fewer/more boards are present.
HOSTS = ['roach%d.solar.pvt' % i for i in range(1, 9)]

# BOF file (bitstream) to load onto each ROACH.
# Must be accessible on the machine running this script.
BOF   = 'eovsa_corr.bof'

# KATCP default port number for ROACH
PORT  = 7147


def extract_devnames(res):
    """
    Extract device names from the return value of `fpga.listdev()`.
    Different versions of katcp may return:
      - a list of inform objects (with .arguments),
      - a tuple (reply, informs) or (reply, informs, errors),
      - plain strings or tuples.
    This function normalizes all those cases to a flat list of strings.
    """
    names = []

    def pull(x):
        # Case 1: Inform/message object with .arguments attribute
        if hasattr(x, 'arguments') and x.arguments:
            return [x.arguments[0]]
        # Case 2: Tuple like ('device_name', 'addr', ...)
        if isinstance(x, tuple) and len(x) >= 1 and isinstance(x[0], basestring):
            return [x[0]]
        # Case 3: Plain string already
        if isinstance(x, basestring):
            return [x]
        return []

    # Handle list/tuple/single object return types
    if isinstance(res, list):
        for item in res:
            names.extend(pull(item))
    elif isinstance(res, tuple):
        for part in res:
            if isinstance(part, (list, tuple)):
                for item in part:
                    names.extend(pull(item))
            else:
                names.extend(pull(part))
    else:
        names.extend(pull(res))

    # Deduplicate while preserving order
    seen = set()
    out = []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out

def list_device_names(fpga, tries=3, delay=1.5):
    """
    Query the FPGA for its loaded devices using `listdev`.
    Retries a few times with a short delay to allow device table to populate
    right after a bitstream load.
    """
    for _ in range(tries):
        try:
            res = fpga.listdev()
            names = extract_devnames(res)
            if names:
                return names
        except Exception as e:
            # Can fail if devices not yet ready; wait and retry
            time.sleep(delay)
        time.sleep(delay)

    # Final attempt, with debug print if still failing
    try:
        res = fpga.listdev()
        print('listdev raw repr: %r' % (res,))
        return extract_devnames(res)
    except Exception as e:
        print('listdev error (final): %s' % e)
        return []

def load_one(host):
    """
    Connect to a single ROACH and attempt to load the BOF design.
    Retries up to 3 times if progdev times out or devices not visible.
    Returns True on success, False otherwise.
    """
    print('=== %s ===' % host)
    try:
        # Try constructor with explicit port; fallback if signature differs
        try:
            fpga = FpgaClient(host, PORT, timeout=10.0)
        except TypeError:
            fpga = FpgaClient(host, timeout=10.0)

        # Wait up to 15s for the TCP/KATCP connection
        fpga.wait_connected(15)
        print('connected: %s' % fpga.is_connected())

        # Increase default per-request timeout (important for progdev)
        if hasattr(fpga, '_timeout'):
            fpga._timeout = max(getattr(fpga, '_timeout', 3.0), 10.0)

    except Exception as e:
        print('connect error: %s' % e)
        return False

    # Try loading the BOF design up to 3 times
    for attempt in range(1, 4):
        try:
            print('progdev attempt %d...' % attempt)
            fpga.progdev(BOF)       # load bitstream; no timeout kwarg in this API
            time.sleep(2.0)         # give devices time to enumerate

            # Query device list
            devs = list_device_names(fpga, tries=4, delay=1.0)
            print('devices: %d' % len(devs))

            # Success if we see kat_adc_controller or any devices at all
            if any('kat_adc_controller' in d for d in devs) or devs:
                print('design loaded; proceeding')
                return True
            else:
                print('design not visible yet; retrying...')

        except Exception as e:
            print('progdev error: %s' % e)
            time.sleep(2.0)

    # All attempts failed
    return False


def load():
    """
    Load the BOF design onto all ROACH boards sequentially.
    Prints the status for each board.
    """
    # --------------------------------------------------------------------
    # Main loop: run sequentially through all ROACH hosts
    # --------------------------------------------------------------------
    for h in HOSTS:
        ok = load_one(h)
        print('%s %s' % (h, 'OK' if ok else 'FAILED'))