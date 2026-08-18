"""
Lightweight helpers to replace measured FEM power values with modeled
attenuation->power predictions when they exceed a threshold.

Coefficient placeholders are embedded so this module can be used without
reading files. Coefficients are first-order: Power(dBm) = c1*attn_total + c0.
Slopes (c1) are fixed per pol/ND; intercepts (c0) vary by env tag: lab
(default), sun, sky.
"""

import numpy as np

try:
    string_types = (basestring,)
except NameError:
    string_types = (str,)

# Placeholder first-order polynomial coefficients.
# Model: Power(dBm) = c1 * attn_total + c0
# c1 (slope) is fixed per pol/ND; c0 (intercept) depends on env tag: "lab",
# "sun", or "sky".
COEFF_SLOPE = {
    "H": {"OFF": -1.0177794117647057, "ON": -1.0048897058823532},
    "V": {"OFF": -1.0404926470588238, "ON": -1.0209411764705887},
}

COEFF_INTERCEPT = {
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
    if s in ("1", "ON", "NDON", "ND_ON"):
        return "ON"
    if s in ("0", "OFF", "NDOFF", "ND_OFF"):
        return "OFF"
    raise ValueError("nd_state must be OFF/ON or 0/1")


def _normalize_env(env):
    e = str(env).strip().lower()
    if e in ("lab", "sun", "sky"):
        return e
    raise ValueError("env must be one of: lab, sun, sky")


def predict_power_from_attn(attn1, attn2, pol, nd_state, env="lab"):
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
    """
    pol_key = _normalize_pol(pol)
    nd_key = _normalize_nd(nd_state)
    env_key = _normalize_env(env)
    c1 = COEFF_SLOPE[pol_key][nd_key]
    c0 = COEFF_INTERCEPT[env_key][pol_key][nd_key]
    attn_total = float(attn1) + float(attn2)
    return float(c1 * attn_total + c0)


def replace_power_if_needed(measured_dbm, attn1, attn2, pol, nd_state, threshold_value, env="lab", measured_voltage=None):
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

    if np.isnan(compare_value) or compare_value <= threshold_value:
        return measured_dbm, False

    modeled = predict_power_from_attn(attn1, attn2, pol, nd_state, env=env)
    return modeled, True
