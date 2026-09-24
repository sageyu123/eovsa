#!/usr/bin/env python
"""Read-only FEM stateframe logger for an Ant14 or Ant15 field test."""

from __future__ import print_function

import argparse
import sys
import time

import stateframe as stf
from util import Time


DEFAULT_ANTENNA = 15


def read_antenna(accini, antenna):
    data, message = stf.get_stateframe(accini)
    if message != "No Error":
        raise RuntimeError(message)

    fem = accini["sf"]["Antenna"][antenna - 1]["Frontend"]["FEM"]

    def pol_values(pol):
        node = fem[pol + "Pol"]
        return (
            stf.extract(data, node["Attenuation"]["First"]),
            stf.extract(data, node["Attenuation"]["Second"]),
            stf.extract(data, node["Voltage"]),
            stf.extract(data, node["Power"]),
        )

    return stf.extract(data, fem["ND"]), pol_values("H"), pol_values("V")


def read_ant15(accini):
    """Preserve the original Ant15 helper for existing callers."""
    return read_antenna(accini, DEFAULT_ANTENNA)


def main():
    parser = argparse.ArgumentParser(
        description="Log FEM ND, attenuation, voltage, and power without commands."
    )
    parser.add_argument("--ant", type=int, default=DEFAULT_ANTENNA)
    parser.add_argument("--duration", type=float, default=420.0)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    if args.ant < 1 or args.ant > 16:
        parser.error("ant must be in the range 1 to 16")
    if args.duration <= 0 or args.interval <= 0:
        parser.error("duration and interval must be positive")

    accini = stf.rd_ACCfile()
    deadline = time.time() + args.duration
    rows = 0

    with open(args.output, "w") as output:
        output.write(
            "utc,nd,h_attn1,h_attn2,h_voltage,h_power,"
            "v_attn1,v_attn2,v_voltage,v_power\n"
        )
        output.flush()

        try:
            while time.time() < deadline:
                started = time.time()
                try:
                    nd_state, h_values, v_values = read_antenna(accini, args.ant)
                    values = (Time.now().iso, nd_state) + h_values + v_values
                    output.write(
                        "%s,%s,%s,%s,%.8g,%.8g,%s,%s,%.8g,%.8g\n" % values
                    )
                    output.flush()
                    rows += 1
                except Exception as exc:
                    print("Stateframe read failed: %s" % exc, file=sys.stderr)

                remaining = args.interval - (time.time() - started)
                if remaining > 0:
                    time.sleep(remaining)
        except KeyboardInterrupt:
            print("Logger interrupted by operator.", file=sys.stderr)

    print(
        "Wrote %d Ant%d rows to %s" % (rows, args.ant, args.output),
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
