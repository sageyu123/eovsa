# FEM14/FEM15 mounted benchmark test

This is the minimal workflow for an Ant14 or Ant15 FEM test after the FEM is
mounted on the antenna. It checks the FEM response and the display/model
handoff. It does **not** update the cRIO, derive the final on-sky calibration,
or return an antenna to observing mode.

## Files

- `ANT14_AFTER_CRIO.md`: short end-to-end Ant14 instructions after loading the
  detector coefficients into the cRIO.
- `FEM14FIELDTEST.ctl`: Ant14-only ND/attenuator sweep (about 4 minutes).
- `FEM14SAFE.ctl`: Ant14-only conservative hold state for an abort.
- `FEM14FIELDTEST.scd.template`: one Ant14 scheduler entry.
- `FEM15FIELDTEST.ctl`: Ant15-only ND/attenuator sweep (about 4 minutes).
- `FEM15SAFE.ctl`: Ant15-only conservative hold state for an abort.
- `FEM15FIELDTEST.scd.template`: one Ant15 scheduler entry.
- `fem15_stateframe_logger.py`: read-only Ant14/Ant15 CSV logger.
- `../../sf_display.py` and `../../attn_power_model.py`: use these GitHub-tracked
  files from the same checkout. Do not copy laptop versions over them.

The FEM display/model behavior used here first appears in commit
`a8253ebd9737c0abb79bb55a34c88a376545f477`. Verify that the server checkout
contains that commit or a later one:

```bash
git merge-base --is-ancestor a8253ebd9737c0abb79bb55a34c88a376545f477 HEAD
```

No local benchmark script is required. In particular, `benchmark_mod_v4.py` is
not included because it drives a USB/pyvisa power meter and is for the lab
bench, not this mounted field test.

Ant15 has an embedded attenuation model. Ant14 passes measured power through
unchanged until its field calibration has been reviewed and added to
`attn_power_model.py`.

## Run

1. Set `ANT=14` or `ANT=15`, obtain operator approval, take that antenna out of
   service, and record its initial FEM/DCM/ND/auto state. Keep an operator at
   the scheduler throughout the test.
2. From the server's checked-out `eovsa` repository, install the two control
   files where the scheduler resolves `.ctl` files:

   ```bash
   cd /home/sched/Dropbox/PythonCode/Current
   ANT=14
   cp --no-clobber tutorials/fem15BMtest/FEM${ANT}FIELDTEST.ctl .
   cp --no-clobber tutorials/fem15BMtest/FEM${ANT}SAFE.ctl .
   ```

   Stop if either destination already exists; compare it before replacing it.
3. Start the read-only logger in terminal A:

   ```bash
   cd /home/sched/Dropbox/PythonCode/Current
   ANT=14
   python tutorials/fem15BMtest/fem15_stateframe_logger.py \
     --ant ${ANT} --duration 420 --output /tmp/fem${ANT}_fieldtest.csv
   ```

   Launch the repository's display in terminal B:

   ```bash
   cd /home/sched/Dropbox/PythonCode/Current
   python sf_display.py
   ```
4. Copy `FEM${ANT}FIELDTEST.scd.template` to a `.scd` file, replace the
   placeholder with a future local server time, and load it through the normal
   scheduler. Before `GO`, confirm the expanded schedule contains hardware
   commands for only the selected antenna. Do not run the old Ant4 test or a
   schedule containing `REWIND`.
5. Watch the selected antenna in `sf_display.py`. On any unexpected antenna,
   backend, or FEM behavior, abort and run `FEM${ANT}SAFE`; then leave the
   antenna out of service.
6. The normal test also ends with ND off, FEM H/V at `31 31`, DCM at `31 31`,
   and FEM/DCM auto disabled. Only the operator should restore the recorded
   initial state after reviewing the result.

## Quick checks

- Only the selected antenna changes, and reported ND/attenuator states follow
  the schedule.
- Voltage/power decreases as attenuation increases; no NaN or stateframe errors.
- For an antenna with a reviewed model, `sf_display.py` shows measured power
  without `*` below its voltage threshold and modeled power with `*` above it.
- Before the Ant14 model is added, Ant14 continues to show measured power; use
  the CSV from this procedure to derive its model.
- Retain the CSV, executed `.ctl`/`.scd`, scheduler log, and display screenshot.

The current display calls the `lab` model, while the `sun`/`sky` intercepts in
`attn_power_model.py` remain placeholders. Therefore this run is a functional
field check, not the final absolute on-sky calibration.
