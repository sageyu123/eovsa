# FEM15 mounted benchmark test

This is the minimal workflow for the first FEM15 test after it is mounted on the
antenna. It checks the FEM response and the Ant15 display/model handoff. It does
**not** update the cRIO, derive the final on-sky calibration, or return Ant15 to
observing mode.

## Files

- `FEM15FIELDTEST.ctl`: Ant15-only ND/attenuator sweep (about 4 minutes).
- `FEM15SAFE.ctl`: Ant15-only conservative hold state for an abort.
- `FEM15FIELDTEST.scd.template`: one scheduler entry; replace the timestamp.
- `fem15_stateframe_logger.py`: read-only Ant15 CSV logger.
- `../../sf_display.py` and `../../attn_power_model.py`: use these GitHub-tracked
  files from the same checkout. Do not copy laptop versions over them.

The FEM display/model behavior used here first appears in commit
`a8253ebd9737c0abb79bb55a34c88a376545f477`. Verify that the server checkout
contains that commit or a later one:

```bash
git merge-base --is-ancestor a8253ebd9737c0abb79bb55a34c88a376545f477 HEAD
```

No local benchmark script is required. In particular, `benchmark_mod_v4.py` is
not included because it drives a USB/pyvisa power meter and is for the lab bench,
not this mounted field test.

## Run

1. Obtain operator approval, take Ant15 out of service, and record its initial
   FEM/DCM/ND/auto state. Keep an operator at the scheduler throughout the test.
2. From the server's checked-out `eovsa` repository, install the two control
   files where the scheduler resolves `.ctl` files:

   ```bash
   cd /home/sched/Dropbox/PythonCode/Current
   cp --no-clobber tutorials/fem15BMtest/FEM15FIELDTEST.ctl .
   cp --no-clobber tutorials/fem15BMtest/FEM15SAFE.ctl .
   ```

   Stop if either destination already exists; compare it before replacing it.
3. Start the read-only logger in terminal A:

   ```bash
   cd /home/sched/Dropbox/PythonCode/Current
   python tutorials/fem15BMtest/fem15_stateframe_logger.py \
     --duration 420 --output /tmp/fem15_fieldtest.csv
   ```

   Launch the repository's display in terminal B:

   ```bash
   cd /home/sched/Dropbox/PythonCode/Current
   python sf_display.py
   ```
4. Copy `FEM15FIELDTEST.scd.template` to a `.scd` file, replace the placeholder
   with a future local server time, and load it through the normal scheduler.
   Before `GO`, confirm the expanded schedule contains only `ANT15` hardware
   commands. Do not run the old Ant4 test or a schedule containing `REWIND`.
5. Watch Ant15 in `sf_display.py`. On any unexpected antenna, backend, or FEM
   behavior, abort and run `FEM15SAFE`; then leave Ant15 out of service.
6. The normal test also ends with ND off, FEM H/V at `31 31`, DCM at `31 31`,
   and FEM/DCM auto disabled. Only the operator should restore the recorded
   initial state after reviewing the result.

## Quick checks

- Only Ant15 changes, and reported ND/attenuator states follow the schedule.
- Voltage/power decreases as attenuation increases; no NaN or stateframe errors.
- At measured voltage `<= 1.105 V`, `sf_display.py` shows measured power without
  `*`; above it, the modeled value has `*`.
- Retain the CSV, executed `.ctl`/`.scd`, scheduler log, and display screenshot.

The current display calls the `lab` model, while the `sun`/`sky` intercepts in
`attn_power_model.py` remain placeholders. Therefore this run is a functional
field check, not the final absolute on-sky calibration.
