# Ant14 test after the cRIO coefficients are loaded

## 1. Get the test code

```bash
cd /home/sched/Dropbox/PythonCode/Current
git fetch origin
git checkout ant14-fem-display-support
git pull --ff-only
```

## 2. Install the schedule files

```bash
cp --no-clobber tutorials/fem15BMtest/FEM14FIELDTEST.ctl .
cp --no-clobber tutorials/fem15BMtest/FEM14SAFE.ctl .
cp tutorials/fem15BMtest/FEM14FIELDTEST.scd.template FEM14FIELDTEST.scd
nano FEM14FIELDTEST.scd
```

Replace the time in `FEM14FIELDTEST.scd` with a future local time.

## 3. Run the test

Start the logger:

```bash
python tutorials/fem15BMtest/fem15_stateframe_logger.py \
  --ant 14 --duration 420 \
  --output /tmp/fem14_fieldtest.csv
```

In a seperate terminal:

```bash
python sf_display.py
```

Load `FEM14FIELDTEST.scd` in the scheduler and press `GO`. If something look
wrong, abort and run:

```text
FEM14SAFE
```

## 4. Fit the Ant14 model

Run this after the test. The default voltage window removes the low voltage
floor and stays below saturation.

```bash
python attn_power_model.py \
  --fit-fieldtest /tmp/fem14_fieldtest.csv \
  --antenna 14
```

If the test plot show a different valid voltage range, add
`--voltage-min VALUE --voltage-max VALUE`.

Copy the printed block below the constant definitions in
`attn_power_model.py`:

```bash
nano attn_power_model.py
```

## 5. Test and check SF Display

```bash
python -m unittest -v test_attn_power_model
python sf_display.py
```

Above the threshold, the Ant14 modeled power should have `*`. Below the
threshold it should show the measured cRIO power without `*`.

## 6. Commit and push

```bash
git add attn_power_model.py
git commit -m "Add Ant14 FEM power calibration"
git push
```
