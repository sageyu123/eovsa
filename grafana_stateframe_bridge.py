#!/usr/bin/env python
"""
Expose a small subset of the live stateframe for Prometheus scraping and ad hoc
JSON inspection. The bridge reuses the existing stateframe utilities that power
sf_display, so it does not change any control logic.

Example:
    $ python grafana_stateframe_bridge.py --port 9105 --poll-interval 2

Once running, point Prometheus at:
    http://<host>:9105/metrics
"""

import argparse
import copy
import glob
import json
import os
import re
import sys
import threading
import time

import numpy as np

import stateframe as stf
from util import Time

if sys.version_info[0] == 2:
    import BaseHTTPServer

    BaseHTTPRequestHandler = BaseHTTPServer.BaseHTTPRequestHandler
    HTTPServer = BaseHTTPServer.HTTPServer
    string_types = (basestring,)  # noqa: F821
    import urllib2 as urllib_request  # type: ignore
else:
    from http.server import BaseHTTPRequestHandler, HTTPServer

    string_types = (str,)
    import urllib.request as urllib_request


def _lv_to_unix_ms(lv_timestamp):
    """Convert a LabVIEW timestamp to milliseconds since Unix epoch."""
    try:
        return int(Time(lv_timestamp, format='lv').unix * 1000.0)
    except Exception:
        return None


def _finite_or_none(value):
    """Return a finite float for scalars/length-1 arrays; otherwise None."""
    try:
        arr = np.asarray(value, dtype=float)
        if arr.size != 1:
            return None
        val = float(arr.reshape(-1)[0])
        return val if np.isfinite(val) else None
    except Exception:
        return None


def _safe_time(lv_timestamp):
    """Convert a LabVIEW timestamp into ISO format if it is valid (> 0)."""
    try:
        if lv_timestamp and lv_timestamp > 0:
            return Time(lv_timestamp, format='lv').iso
    except Exception:
        pass
    return None


def _prometheus_escape(value):
    """Escape label values for Prometheus text exposition format."""
    text = '' if value is None else str(value)
    return text.replace('\\', r'\\').replace('\n', r'\n').replace('"', r'\"')


def _prometheus_format_value(value):
    """Format floats/bools in a Prometheus-compatible way."""
    if isinstance(value, bool):
        return '1' if value else '0'
    try:
        number = float(value)
    except Exception:
        return None
    if np.isnan(number):
        return None
    if np.isposinf(number):
        return '+Inf'
    if np.isneginf(number):
        return '-Inf'
    return repr(number)


def _prometheus_sample(name, value, labels=None):
    """Return one Prometheus sample line or None if the value is invalid."""
    formatted = _prometheus_format_value(value)
    if formatted is None:
        return None
    if labels:
        items = sorted(labels.items())
        rendered = ','.join(
            '{}="{}"'.format(key, _prometheus_escape(val))
            for key, val in items)
        return '{}{{{}}} {}'.format(name, rendered, formatted)
    return '{} {}'.format(name, formatted)


class FlareMonitorReader(object):
    """Tail flaretest files to expose the latest detector measurements."""

    def __init__(self, root='/data1/eovsa/fits/FTST', lookback_days=3,
                 base_url='https://ovsa.njit.edu/fits/FTST'):
        self.root = os.environ.get('FLAREMON_ROOT', root)
        self.base_url = os.environ.get('FLAREMON_URL', base_url).rstrip('/')
        self.lookback_days = max(1, int(lookback_days))
        self._current_file = None
        self._current_meta = {}

    def latest(self):
        path = self._find_latest_file()
        if path is None:
            return {'error': 'No flaretest files found'}
        if path != self._current_file:
            self._current_meta = self._read_header(path)
            self._current_file = path
        sample = self._read_last_sample(path)
        result = dict(self._current_meta)
        result['file_path'] = path
        if sample is None:
            result.setdefault('error', 'No samples found in {}'.format(os.path.basename(path)))
            return result
        result.update(sample)
        result.setdefault('error', None)
        return result

    def _find_latest_file(self):
        files = []
        now = Time.now()
        if os.path.isdir(self.root):
            for delta in range(self.lookback_days):
                day = Time(now.mjd - delta, format='mjd').iso[:10]
                files.extend(self._local_files_for_day(day))
            if files:
                try:
                    return max(files, key=os.path.getmtime)
                except Exception:
                    return sorted(files)[-1]
        # Local files not available-fall back to HTTP listing.
        for delta in range(self.lookback_days):
            day = Time(now.mjd - delta, format='mjd').iso[:10]
            files.extend(self._remote_files_for_day(day))
        return files[0] if files else None

    def _local_files_for_day(self, datestr):
        year = datestr[:4]
        month = datestr[5:7]
        day = datestr[8:10]
        yymmdd = datestr[2:4] + month + day
        pattern = os.path.join(self.root, year, month, 'flaretest_{}*.txt'.format(yymmdd))
        return glob.glob(pattern)

    def _remote_files_for_day(self, datestr):
        if not self.base_url:
            return []
        year = datestr[:4]
        month = datestr[5:7]
        prefix = '{}/{}/{}/'.format(self.base_url, year, month)
        url = prefix
        try:
            resp = urllib_request.urlopen(url, timeout=10)
            body = resp.read()
            try:
                body = body.decode('utf-8', errors='ignore')
            except AttributeError:
                body = body
        except Exception:
            return []
        matches = re.findall(r'flaretest_\d+\.txt', body)
        unique = sorted(set(matches), reverse=True)
        return ['{}{}'.format(prefix, name) for name in unique]

    def _read_header(self, path):
        meta = {
            'source_id': None,
            'project': None,
            'scan_id': None,
            'freq_band_ghz': None,
            'avg_time_s': None,
            'threshold_time_s': None,
            'nsigmas': None,
        }
        try:
            for line in self._iter_lines(path, max_lines=12):
                clean = line.strip().replace('\x00', '')
                if not clean:
                    continue
                lower = clean.lower()
                if lower.startswith('date'):
                    break
                if clean.startswith('SOURCEID'):
                    meta['source_id'] = clean.split(':', 1)[1].strip()
                elif clean.startswith('PROJECT'):
                    meta['project'] = clean.split(':', 1)[1].strip()
                elif clean.startswith('SCANID'):
                    meta['scan_id'] = clean.split(':', 1)[1].strip()
                elif clean.startswith('FREQ BAND'):
                    try:
                        meta['freq_band_ghz'] = [float(val) for val in clean.split(':', 1)[1].split()]
                    except Exception:
                        meta['freq_band_ghz'] = None
                elif clean.startswith('Avg. Time'):
                    try:
                        meta['avg_time_s'] = float(clean.split(':', 1)[1])
                    except Exception:
                        meta['avg_time_s'] = None
                elif clean.startswith('Threshold Time'):
                    try:
                        meta['threshold_time_s'] = float(clean.split(':', 1)[1])
                    except Exception:
                        meta['threshold_time_s'] = None
                elif clean.startswith('Nsigmas'):
                    try:
                        meta['nsigmas'] = float(clean.split()[-1])
                    except Exception:
                        meta['nsigmas'] = None
        except Exception:
            pass
        return meta

    def _read_last_sample(self, path):
        last_line = None
        try:
            for line in self._iter_lines(path):
                clean = line.strip().replace('\x00', '')
                if not clean:
                    continue
                if clean[0].isdigit():
                    last_line = clean
        except Exception:
            return None
        if not last_line:
            return None
        parts = last_line.split()
        if len(parts) < 10:
            return None
        date_str, time_str, flag = parts[:3]
        detector_vals = []
        for value in parts[3:6]:
            try:
                detector_vals.append(float(value))
            except Exception:
                detector_vals.append(None)
        try:
            mean_val = float(parts[6])
        except Exception:
            mean_val = None
        try:
            sigma_val = float(parts[7])
        except Exception:
            sigma_val = None
        try:
            threshold = float(parts[8])
        except Exception:
            threshold = None
        try:
            count = float(parts[9])
        except Exception:
            count = None
        timestamp_iso, timestamp_ms = self._parse_datetime(date_str, time_str)
        age_seconds = None
        if timestamp_ms is not None:
            age_seconds = max(0.0, Time.now().unix - (timestamp_ms / 1000.0))
        return {
            'timestamp_iso': timestamp_iso,
            'timestamp_unix_ms': timestamp_ms,
            'age_seconds': age_seconds,
            'flag': flag,
            'flag_active': flag.upper() not in ('F', '0', 'FALSE'),
            'detectors': detector_vals,
            'mean': mean_val,
            'sigma': sigma_val,
            'threshold': threshold,
            'count': count
        }

    def _parse_datetime(self, datestr, timestr):
        try:
            year = datestr[:4]
            month = datestr[4:6]
            day = datestr[6:8]
            hour = timestr[:2]
            minute = timestr[2:4]
            second = timestr[4:]
            timestr_iso = '{}-{}-{} {}:{}:{}'.format(year, month, day, hour, minute, second)
            t = Time(timestr_iso)
            return t.iso, int(t.unix * 1000.0)
        except Exception:
            return None, None

    def _iter_lines(self, path, max_lines=None):
        """Yield text lines from local files or HTTP URLs."""
        handle = None
        try:
            if path.startswith('http'):
                handle = urllib_request.urlopen(path, timeout=10)
                iterator = handle
            else:
                handle = open(path, 'r')
                iterator = handle
            count = 0
            for raw in iterator:
                if isinstance(raw, bytes):
                    line = raw.decode('utf-8', errors='ignore')
                else:
                    line = raw
                yield line
                count += 1
                if max_lines is not None and count >= max_lines:
                    break
        finally:
            try:
                if handle:
                    handle.close()
            except Exception:
                pass


class StateframeSampler(object):
    """Continuously sample the ACC stateframe and keep the latest payload."""

    def __init__(self, poll_interval=1.0, antlist=None):
        self.accini = stf.rd_ACCfile(host='ovsa')
        self.sf = self.accini['sf']
        if antlist is None:
            antlist = range(16)
        self.antlist = antlist
        self.poll_interval = poll_interval
        self._lock = threading.Lock()
        self._latest = {'error': 'Sampler not started'}
        self.flare_reader = FlareMonitorReader()
        self._thread = threading.Thread(target=self._loop)
        self._thread.daemon = True
        self._thread.start()

    def latest(self):
        with self._lock:
            return copy.deepcopy(self._latest)

    def prometheus_payload(self):
        """Return a copy of the latest payload for Prometheus rendering."""
        with self._lock:
            return copy.deepcopy(self._latest)

    def _loop(self):
        while True:
            data, msg = stf.get_stateframe(self.accini)
            if msg == 'No Error':
                payload = self._build_payload(data)
            else:
                payload = {
                    'error': msg,
                    'timestamp_iso': Time.now().iso
                }
            with self._lock:
                self._latest = payload
            time.sleep(self.poll_interval)

    def _build_payload(self, data):
        sf = self.sf
        payload = {}
        sf_ts = stf.extract(data, sf['Timestamp'])
        payload['stateframe_timestamp_lv'] = int(sf_ts)
        payload['stateframe_time_iso'] = _safe_time(sf_ts)
        payload['stateframe_time_unix_ms'] = _lv_to_unix_ms(sf_ts)

        sched_ts = stf.extract(data, sf['Schedule']['Data']['Timestamp'])
        payload['schedule_timestamp_lv'] = int(sched_ts)
        payload['schedule_time_iso'] = _safe_time(sched_ts)
        payload['schedule_time_unix_ms'] = _lv_to_unix_ms(sched_ts)

        raw_task = stf.extract(data, sf['Schedule']['Task']).strip('\x00').replace('\t', ' ').replace('\r\n', '|')
        task = raw_task
        if task:
            # Strip the trailing delimiter and the leading epoch stamp.
            task = task[:-1]
            parts = task.split()
            task = ' '.join(parts[1:]) if len(parts) > 1 else ''
        payload['task'] = task

        payload['weather'] = self._weather_block(data)
        payload['solar_power'] = self._solar_power_block(data, sf_ts)
        payload['roach'] = self._roach_block(data)
        payload['antennas'] = self._antenna_block(data)
        payload['flare_monitor'] = self.flare_reader.latest()
        payload['error'] = None
        return payload

    def _weather_block(self, data):
        weather = self.sf['Schedule']['Data']['Weather']
        dtor = np.pi / 180.
        wind = float(stf.extract(data, weather['Wind']))
        avg_wind = float(stf.extract(data, weather['AvgWind']))
        direction = stf.extract(data, weather['WindDirection']) / dtor
        dirs = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
        idir = int(np.fmod(direction + 22.5, 360.) / 45.)
        return {
            'wind_mph': wind,
            'avg_wind_mph': avg_wind,
            'wind_direction_deg': float(direction),
            'wind_direction_cardinal': dirs[idir],
            'temperature_f': float(stf.extract(data, weather['Temperature'])),
            'pressure_mbar': float(stf.extract(data, weather['Pressure']))
        }

    def _solar_power_block(self, data, sf_timestamp):
        now = Time(sf_timestamp, format='lv')
        block = []
        for idx, key in enumerate(self.sf['Schedule']['Data']['SolarPower']):
            reading_ts = stf.extract(data, key['Timestamp'])
            entry = {
                'array': 12 if idx == 0 else 13,
                'timestamp_lv': int(reading_ts),
                'timestamp_iso': _safe_time(reading_ts),
                'charge_pct': float(stf.extract(data, key['Charge'])),
                'voltage_v': float(stf.extract(data, key['Volts'])),
                'current_a': float(stf.extract(data, key['Amps']))
            }
            if entry['timestamp_iso']:
                age = (now - Time(reading_ts, format='lv')).value * 86400.0
                entry['age_seconds'] = float(age)
            else:
                entry['age_seconds'] = None
            block.append(entry)
        return block

    def _roach_block(self, data):
        # Map the ambient temperature into F, mirroring sf_display.
        ambient_c = stf.extract(data, self.sf['Schedule']['Data']['Roach'][0]['Temp.ambient'])
        temp_f = int(ambient_c * 90. / 5) / 10. + 32
        return {'control_room_temp_f': float(temp_f)}

    def _antenna_block(self, data):
        stats = stf.azel_from_stateframe(self.sf, data, self.antlist)
        antennas = []
        for idx, ant in enumerate(self.antlist):
            az_actual = float(stats['ActualAzimuth'][idx])
            el_actual = float(stats['ActualElevation'][idx])
            tracking = bool(stats['TrackFlag'][idx])
            # If both axes are parked at zero, treat the antenna as not tracking.
            if abs(az_actual) < 1e-6 and abs(el_actual) < 1e-6:
                tracking = False
            c = self.sf['Antenna'][ant]['Controller']
            try:
                ra_requested = _finite_or_none(stf.extract(data, c['RAVirtualAxis']) / 10000.0)
            except Exception:
                ra_requested = None
            try:
                dec_requested = _finite_or_none(stf.extract(data, c['DecVirtualAxis']) / 10000.0)
            except Exception:
                dec_requested = None
            fe = self.sf['Antenna'][ant]['Frontend']['FEM']
            try:
                fe_h_power = _finite_or_none(stf.extract(data, fe['HPol']['Power']))
            except Exception:
                fe_h_power = None
            try:
                fe_v_power = _finite_or_none(stf.extract(data, fe['VPol']['Power']))
            except Exception:
                fe_v_power = None
            be = self.sf['DCM'][ant]
            try:
                be_h_voltage = _finite_or_none(stf.extract(data, be['HPol']['Voltage']))
            except Exception:
                be_h_voltage = None
            try:
                be_v_voltage = _finite_or_none(stf.extract(data, be['VPol']['Voltage']))
            except Exception:
                be_v_voltage = None
            antennas.append({
                'id': ant + 1,
                'az_actual_deg': az_actual,
                'az_requested_deg': float(stats['RequestedAzimuth'][idx]),
                'ra_requested_deg': ra_requested,
                'dec_requested_deg': dec_requested,
                'el_actual_deg': el_actual,
                'el_requested_deg': float(stats['RequestedElevation'][idx]),
                'delta_az_deg': float(stats['dAzimuth'][idx]),
                'delta_el_deg': float(stats['dElevation'][idx]),
                'parallactic_angle_deg': float(stats['ParallacticAngle'][idx]),
                'tracking': tracking,
                'track_source': bool(stats['TrackSrcFlag'][idx]),
                'fe_hpol_power_dbm': fe_h_power,
                'fe_vpol_power_dbm': fe_v_power,
                'be_hpol_voltage_v': be_h_voltage,
                'be_vpol_voltage_v': be_v_voltage
            })
        return antennas


SAMPLER = None


class GrafanaRequestHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler providing latest payload, health, and Prometheus metrics."""

    def log_message(self, fmt, *args):
        # Silence default logging noise.
        pass

    def _send_json(self, payload, status=200):
        body = json.dumps(payload)
        if isinstance(body, bytes):
            body_bytes = body
        else:
            body_bytes = body.encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body_bytes))
        self.end_headers()
        self.wfile.write(body_bytes)

    def _send_text(self, body, status=200, content_type='text/plain; version=0.0.4; charset=utf-8'):
        if isinstance(body, bytes):
            body_bytes = body
        else:
            body_bytes = body.encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', len(body_bytes))
        self.end_headers()
        self.wfile.write(body_bytes)

    def do_GET(self):
        if self.path in ('/', '/stateframe'):
            payload = SAMPLER.latest()
            self._send_json(payload)
        elif self.path == '/healthz':
            payload = SAMPLER.latest()
            status = 200 if payload.get('error') in (None, 'Sampler not started') else 503
            self._send_json({'status': payload.get('error') or 'ok'}, status=status)
        elif self.path == '/metrics':
            self._send_text(self._handle_metrics())
        else:
            self._send_json({'error': 'unknown endpoint'}, status=404)

    def _handle_metrics(self):
        payload = SAMPLER.prometheus_payload()
        error = payload.get('error')
        lines = [
            '# HELP eovsa_bridge_up Whether the Grafana bridge has a valid latest sample.',
            '# TYPE eovsa_bridge_up gauge',
            _prometheus_sample('eovsa_bridge_up', 0 if error else 1)
        ]
        if error:
            lines.extend([
                '# HELP eovsa_bridge_error_info Latest bridge error as an info-style gauge.',
                '# TYPE eovsa_bridge_error_info gauge',
                _prometheus_sample('eovsa_bridge_error_info', 1, {'message': error})
            ])
            return '\n'.join(line for line in lines if line) + '\n'

        stateframe_ms = payload.get('stateframe_time_unix_ms')
        schedule_ms = payload.get('schedule_time_unix_ms')
        lines.extend([
            '# HELP eovsa_stateframe_timestamp_seconds Stateframe timestamp in Unix seconds.',
            '# TYPE eovsa_stateframe_timestamp_seconds gauge',
            _prometheus_sample('eovsa_stateframe_timestamp_seconds',
                               stateframe_ms / 1000.0 if stateframe_ms is not None else None),
            '# HELP eovsa_schedule_timestamp_seconds Scheduler timestamp in Unix seconds.',
            '# TYPE eovsa_schedule_timestamp_seconds gauge',
            _prometheus_sample('eovsa_schedule_timestamp_seconds',
                               schedule_ms / 1000.0 if schedule_ms is not None else None)
        ])

        task = payload.get('task')
        if task:
            lines.extend([
                '# HELP eovsa_schedule_task_info Current scheduler task as a labeled gauge.',
                '# TYPE eovsa_schedule_task_info gauge',
                _prometheus_sample('eovsa_schedule_task_info', 1, {'task': task})
            ])

        weather = payload.get('weather') or {}
        lines.extend([
            '# HELP eovsa_weather_wind_mph Instantaneous wind speed in miles per hour.',
            '# TYPE eovsa_weather_wind_mph gauge',
            _prometheus_sample('eovsa_weather_wind_mph', weather.get('wind_mph')),
            '# HELP eovsa_weather_avg_wind_mph Average wind speed in miles per hour.',
            '# TYPE eovsa_weather_avg_wind_mph gauge',
            _prometheus_sample('eovsa_weather_avg_wind_mph', weather.get('avg_wind_mph')),
            '# HELP eovsa_weather_wind_direction_deg Wind direction in degrees.',
            '# TYPE eovsa_weather_wind_direction_deg gauge',
            _prometheus_sample('eovsa_weather_wind_direction_deg', weather.get('wind_direction_deg')),
            '# HELP eovsa_weather_temperature_f Ambient temperature in Fahrenheit.',
            '# TYPE eovsa_weather_temperature_f gauge',
            _prometheus_sample('eovsa_weather_temperature_f', weather.get('temperature_f')),
            '# HELP eovsa_weather_pressure_mbar Ambient pressure in millibar.',
            '# TYPE eovsa_weather_pressure_mbar gauge',
            _prometheus_sample('eovsa_weather_pressure_mbar', weather.get('pressure_mbar'))
        ])
        wind_cardinal = weather.get('wind_direction_cardinal')
        if wind_cardinal:
            lines.extend([
                '# HELP eovsa_weather_wind_direction_info Wind direction cardinal label.',
                '# TYPE eovsa_weather_wind_direction_info gauge',
                _prometheus_sample('eovsa_weather_wind_direction_info', 1, {'cardinal': wind_cardinal})
            ])

        roach = payload.get('roach') or {}
        lines.extend([
            '# HELP eovsa_control_room_temperature_f Control room temperature in Fahrenheit.',
            '# TYPE eovsa_control_room_temperature_f gauge',
            _prometheus_sample('eovsa_control_room_temperature_f', roach.get('control_room_temp_f'))
        ])

        lines.extend([
            '# HELP eovsa_solar_charge_pct Solar array charge percentage.',
            '# TYPE eovsa_solar_charge_pct gauge',
            '# HELP eovsa_solar_voltage_v Solar array voltage in volts.',
            '# TYPE eovsa_solar_voltage_v gauge',
            '# HELP eovsa_solar_current_a Solar array current in amps.',
            '# TYPE eovsa_solar_current_a gauge',
            '# HELP eovsa_solar_age_seconds Age of the most recent solar array sample in seconds.',
            '# TYPE eovsa_solar_age_seconds gauge'
        ])
        for entry in payload.get('solar_power', []):
            labels = {'array': str(entry.get('array'))}
            lines.append(_prometheus_sample('eovsa_solar_charge_pct', entry.get('charge_pct'), labels))
            lines.append(_prometheus_sample('eovsa_solar_voltage_v', entry.get('voltage_v'), labels))
            lines.append(_prometheus_sample('eovsa_solar_current_a', entry.get('current_a'), labels))
            lines.append(_prometheus_sample('eovsa_solar_age_seconds', entry.get('age_seconds'), labels))

        antenna_metric_defs = [
            ('eovsa_antenna_az_actual_deg', 'az_actual_deg', 'Actual antenna azimuth in degrees.'),
            ('eovsa_antenna_az_requested_deg', 'az_requested_deg', 'Requested antenna azimuth in degrees.'),
            ('eovsa_antenna_ra_requested_deg', 'ra_requested_deg', 'Requested antenna right ascension in degrees.'),
            ('eovsa_antenna_dec_requested_deg', 'dec_requested_deg', 'Requested antenna declination in degrees.'),
            ('eovsa_antenna_el_actual_deg', 'el_actual_deg', 'Actual antenna elevation in degrees.'),
            ('eovsa_antenna_el_requested_deg', 'el_requested_deg', 'Requested antenna elevation in degrees.'),
            ('eovsa_antenna_delta_az_deg', 'delta_az_deg', 'Requested minus actual azimuth in degrees.'),
            ('eovsa_antenna_delta_el_deg', 'delta_el_deg', 'Requested minus actual elevation in degrees.'),
            ('eovsa_antenna_parallactic_angle_deg', 'parallactic_angle_deg', 'Parallactic angle in degrees.'),
            ('eovsa_antenna_tracking', 'tracking', 'Tracking flag for each antenna.'),
            ('eovsa_antenna_track_source', 'track_source', 'Track-source flag for each antenna.'),
            ('eovsa_frontend_hpol_power_dbm', 'fe_hpol_power_dbm', 'Frontend H-pol power in dBm.'),
            ('eovsa_frontend_vpol_power_dbm', 'fe_vpol_power_dbm', 'Frontend V-pol power in dBm.'),
            ('eovsa_backend_hpol_voltage_v', 'be_hpol_voltage_v', 'Backend H-pol voltage in volts.'),
            ('eovsa_backend_vpol_voltage_v', 'be_vpol_voltage_v', 'Backend V-pol voltage in volts.')
        ]
        for metric_name, _, help_text in antenna_metric_defs:
            lines.append('# HELP {} {}'.format(metric_name, help_text))
            lines.append('# TYPE {} gauge'.format(metric_name))
        for antenna in payload.get('antennas', []):
            labels = {'antenna': '{:02d}'.format(int(antenna.get('id')))}
            for metric_name, field_name, _ in antenna_metric_defs:
                lines.append(_prometheus_sample(metric_name, antenna.get(field_name), labels))

        flare = payload.get('flare_monitor') or {}
        lines.extend([
            '# HELP eovsa_flare_timestamp_seconds Timestamp of the latest flare monitor sample in Unix seconds.',
            '# TYPE eovsa_flare_timestamp_seconds gauge',
            _prometheus_sample('eovsa_flare_timestamp_seconds',
                               flare.get('timestamp_unix_ms') / 1000.0
                               if flare.get('timestamp_unix_ms') is not None else None),
            '# HELP eovsa_flare_flag_active Whether the flare detector is active.',
            '# TYPE eovsa_flare_flag_active gauge',
            _prometheus_sample('eovsa_flare_flag_active', flare.get('flag_active')),
            '# HELP eovsa_flare_mean Mean flare detector value.',
            '# TYPE eovsa_flare_mean gauge',
            _prometheus_sample('eovsa_flare_mean', flare.get('mean')),
            '# HELP eovsa_flare_sigma Flare detector sigma.',
            '# TYPE eovsa_flare_sigma gauge',
            _prometheus_sample('eovsa_flare_sigma', flare.get('sigma')),
            '# HELP eovsa_flare_threshold Flare detector threshold.',
            '# TYPE eovsa_flare_threshold gauge',
            _prometheus_sample('eovsa_flare_threshold', flare.get('threshold')),
            '# HELP eovsa_flare_count Flare detector sample count.',
            '# TYPE eovsa_flare_count gauge',
            _prometheus_sample('eovsa_flare_count', flare.get('count')),
            '# HELP eovsa_flare_age_seconds Age of the latest flare detector sample in seconds.',
            '# TYPE eovsa_flare_age_seconds gauge',
            _prometheus_sample('eovsa_flare_age_seconds', flare.get('age_seconds')),
            '# HELP eovsa_flare_detector Flare detector channel value.',
            '# TYPE eovsa_flare_detector gauge'
        ])
        for idx, value in enumerate(flare.get('detectors') or [], 1):
            lines.append(_prometheus_sample('eovsa_flare_detector', value, {'detector': str(idx)}))

        return '\n'.join(line for line in lines if line) + '\n'


def _parse_antennas(arg):
    if not arg:
        return None
    ants = []
    for item in arg.split(','):
        item = item.strip()
        if not item:
            continue
        idx = int(item)
        if idx < 1 or idx > 16:
            raise ValueError('Antenna ids must be 1-16')
        ants.append(idx - 1)
    return ants


def main():
    parser = argparse.ArgumentParser(description='Expose stateframe snippets for Prometheus.')
    parser.add_argument('--port', type=int, default=9105, help='HTTP port to listen on (default: 9105)')
    parser.add_argument('--poll-interval', type=float, default=30.0,
                        help='Seconds between ACC polls (default: 30)')
    parser.add_argument('--antennas', default='', help='Comma-separated antenna IDs (1-16) to publish')
    args = parser.parse_args()

    antlist = _parse_antennas(args.antennas)
    global SAMPLER
    SAMPLER = StateframeSampler(
        poll_interval=args.poll_interval,
        antlist=antlist)

    server = HTTPServer(('', args.port), GrafanaRequestHandler)
    print('Grafana bridge listening on port {}'.format(args.port))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nShutting down Grafana bridge.')
        server.server_close()


if __name__ == '__main__':
    main()
