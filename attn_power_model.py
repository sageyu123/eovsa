"""
Lightweight helpers to replace measured FEM power values with antenna-specific
attenuation->power predictions when they exceed a threshold.

Coefficients are embedded so this module can be used without reading files.
They are first-order: Power(dBm) = c1*attn_total + c0. Antennas without a
calibration pass measured power through unchanged.
"""

import argparse
import csv

import numpy as np

try:
    string_types = (basestring,)
except NameError:
    string_types = (str,)

# First-order polynomial coefficients keyed by one-based antenna number.
# Model: Power(dBm) = c1 * attn_total + c0
# c1 (slope) is fixed per pol/ND; c0 (intercept) depends on env tag: "lab",
# "sun", or "sky".
# Add Ant14 only after its field calibration has been reviewed.
COEFF_SLOPE = {
    15: {
        "H": {"OFF": -1.0177794117647057, "ON": -1.0048897058823532},
        "V": {"OFF": -1.0404926470588238, "ON": -1.0209411764705887},
    },
}

COEFF_INTERCEPT = {
    15: {
        "lab": {
            "H": {"OFF": 9.843970588235292, "ON": 14.109485294117649},
            "V": {"OFF": 10.154007352941177, "ON": 14.060808823529415},
        },
        "sun": {
            "H": {"OFF": 0.0, "ON": 0.0},
            "V": {"OFF": 0.0, "ON": 0.0},
        },
        "sky": {
            "H": {"OFF": 0.0, "ON": 0.0},
            "V": {"OFF": 0.0, "ON": 0.0},
        },
    },
}

VOLTAGE_THRESHOLD = {
    15: {"H": 1.105, "V": 1.105},
}


def _normalize_pol(pol):
    p = str(pol).strip().upper()
    if p.startswith("H"):
        return "H"
    if p.startswith("V"):
        return "V"
    raise ValueError("pol must be H or V")


def _normalize_nd(nd_state):
    if not isinstance(nd_state, string_types):
        try:
            return "ON" if int(nd_state) == 1 else "OFF"
        except (TypeError, ValueError):
            pass
    s = str(nd_state).strip().upper()
    if s in ("1", "ON", "TRUE", "NDON", "ND_ON"):
        return "ON"
    if s in ("0", "OFF", "FALSE", "NDOFF", "ND_OFF"):
        return "OFF"
    raise ValueError("nd_state must be OFF/ON or 0/1")


def _normalize_env(env):
    e = str(env).strip().lower()
    if e in ("lab", "sun", "sky"):
        return e
    raise ValueError("env must be one of: lab, sun, sky")


def get_voltage_threshold(antenna, pol):
    """Return the calibrated voltage threshold, or None when unavailable."""
    try:
        antenna_key = int(antenna)
    except (TypeError, ValueError):
        return None
    pol_key = _normalize_pol(pol)
    return VOLTAGE_THRESHOLD.get(antenna_key, {}).get(pol_key)


def fit_fieldtest_csv(filename, voltage_min=0.05, voltage_max=1.05):
    """Fit H/V, ND OFF/ON power models from a field-test logger CSV."""
    voltage_min = float(voltage_min)
    voltage_max = float(voltage_max)
    if voltage_max <= voltage_min:
        raise ValueError("voltage_max must be greater than voltage_min")

    required_fields = set(["nd"])
    for pol in ("h", "v"):
        required_fields.update([
            pol + "_attn1",
            pol + "_attn2",
            pol + "_voltage",
            pol + "_power",
        ])

    samples = {}
    for pol in ("H", "V"):
        for nd_state in ("OFF", "ON"):
            samples[(pol, nd_state)] = ([], [])

    with open(filename, "r") as input_file:
        reader = csv.DictReader(input_file)
        missing = required_fields.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(
                "Missing CSV columns: %s" % ", ".join(sorted(missing))
            )

        for row_number, row in enumerate(reader, 2):
            try:
                nd_state = _normalize_nd(row["nd"])
                for pol in ("H", "V"):
                    prefix = pol.lower() + "_"
                    voltage = float(row[prefix + "voltage"])
                    power = float(row[prefix + "power"])
                    if not np.isfinite(voltage) or not np.isfinite(power):
                        continue
                    if not voltage_min < voltage <= voltage_max:
                        continue
                    total_attn = (
                        float(row[prefix + "attn1"])
                        + float(row[prefix + "attn2"])
                    )
                    if not np.isfinite(total_attn):
                        continue
                    samples[(pol, nd_state)][0].append(total_attn)
                    samples[(pol, nd_state)][1].append(power)
            except (TypeError, ValueError) as exc:
                raise ValueError("Invalid CSV row %d: %s" % (row_number, exc))

    fits = {}
    for pol in ("H", "V"):
        fits[pol] = {}
        for nd_state in ("OFF", "ON"):
            attenuation, power = samples[(pol, nd_state)]
            if len(attenuation) < 3:
                raise ValueError(
                    "Not enough valid %s ND %s rows (need at least 3)"
                    % (pol, nd_state)
                )
            slope, intercept = np.polyfit(attenuation, power, 1)
            fits[pol][nd_state] = {
                "slope": float(slope),
                "intercept": float(intercept),
                "points_used": len(attenuation),
            }
    return fits


def format_calibration_assignments(antenna, fits, threshold=1.105):
    """Format fitted values for direct insertion into this module."""
    antenna = int(antenna)
    threshold = float(threshold)
    lines = ["COEFF_SLOPE[%d] = {" % antenna]
    for pol in ("H", "V"):
        lines.append(
            '    "%s": {"OFF": %.12g, "ON": %.12g},'
            % (pol, fits[pol]["OFF"]["slope"], fits[pol]["ON"]["slope"])
        )
    lines.extend(["}", "COEFF_INTERCEPT[%d] = {" % antenna, '    "lab": {'])
    for pol in ("H", "V"):
        lines.append(
            '        "%s": {"OFF": %.12g, "ON": %.12g},'
            % (
                pol,
                fits[pol]["OFF"]["intercept"],
                fits[pol]["ON"]["intercept"],
            )
        )
    lines.extend([
        "    },",
        "}",
        'VOLTAGE_THRESHOLD[%d] = {"H": %.12g, "V": %.12g}'
        % (antenna, threshold, threshold),
    ])
    return "\n".join(lines)


def predict_power_from_attn(attn1, attn2, pol, nd_state, env="lab", antenna=15):
    """
    Predict FEM power (dBm) from attenuation settings and ND state.

    Parameters
    ----------
    attn1, attn2 : float or int
        Front-end attenuation settings.
    pol : "H" or "V"
    nd_state : 0/1 or "OFF"/"ON"
    env : {"lab","sun","sky"}
        Select intercept set; slope remains fixed per pol/ND.
    antenna : int
        One-based antenna number with an available calibration.
    """
    pol_key = _normalize_pol(pol)
    nd_key = _normalize_nd(nd_state)
    env_key = _normalize_env(env)
    antenna_key = int(antenna)
    c1 = COEFF_SLOPE[antenna_key][pol_key][nd_key]
    c0 = COEFF_INTERCEPT[antenna_key][env_key][pol_key][nd_key]
    attn_total = float(attn1) + float(attn2)
    return float(c1 * attn_total + c0)


def replace_power_if_needed(measured_dbm, attn1, attn2, pol, nd_state,
                            threshold_value=None, env="lab",
                            measured_voltage=None, antenna=15):
    """
    Replace measured power with modeled power if a measured value exceeds a threshold.

    Returns
    -------
    value : float
        Measured or modeled value.
    replaced : bool
        True if the modeled value was used.
    """
    if measured_dbm is None:
        return measured_dbm, False

    try:
        if np.isnan(measured_dbm):
            return measured_dbm, False
    except TypeError:
        return measured_dbm, False

    compare_value = measured_voltage

    if compare_value is None:
        return measured_dbm, False

    try:
        compare_value = float(compare_value)
    except (TypeError, ValueError):
        return measured_dbm, False

    if threshold_value is None:
        try:
            threshold_value = get_voltage_threshold(antenna, pol)
        except ValueError:
            return measured_dbm, False
    if threshold_value is None:
        return measured_dbm, False

    try:
        threshold_value = float(threshold_value)
    except (TypeError, ValueError):
        return measured_dbm, False

    if np.isnan(compare_value) or compare_value <= threshold_value:
        return measured_dbm, False

    try:
        modeled = predict_power_from_attn(
            attn1, attn2, pol, nd_state, env=env, antenna=antenna
        )
    except (KeyError, TypeError, ValueError):
        return measured_dbm, False
    return modeled, True


def main():
    parser = argparse.ArgumentParser(
        description="Fit FEM power-model coefficients from a field-test CSV."
    )
    parser.add_argument("--fit-fieldtest", required=True, metavar="CSV_FILE")
    parser.add_argument("--antenna", type=int, required=True)
    parser.add_argument("--voltage-min", type=float, default=0.05)
    parser.add_argument("--voltage-max", type=float, default=1.05)
    parser.add_argument("--threshold", type=float, default=1.105)
    args = parser.parse_args()

    try:
        fits = fit_fieldtest_csv(
            args.fit_fieldtest,
            voltage_min=args.voltage_min,
            voltage_max=args.voltage_max,
        )
    except (IOError, ValueError) as exc:
        parser.error(str(exc))
    print(format_calibration_assignments(args.antenna, fits, args.threshold))


if __name__ == "__main__":
    main()
