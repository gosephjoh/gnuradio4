#!/usr/bin/env python3
"""Check gen4 against a cell the GR3 project generated with the same arguments.

    check-gen4.py GR3_CELL GEN4_CELL

Reports: payloads.bin identical (sha256), manifest frames[] identical,
tx_samples.cf32 (if both have it) and rx_stimulus.cf32 max|d| and NMSE per
file.  Expected: identical payloads and frames, max|d| about 6e-7 on both
sample planes (FFTW vs the standalone radix-2 FFT, single precision), which
is far inside the fixture's 1e-5 tolerance.  numpy only.
"""
import hashlib
import json
import os
import sys

import numpy as np


def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    g, c = sys.argv[1], sys.argv[2]
    rc = 0
    same = sha(g + "/payloads.bin") == sha(c + "/payloads.bin")
    print("payloads.bin identical:", same)
    rc |= not same
    mg, mc = json.load(open(g + "/manifest.json")), json.load(open(c + "/manifest.json"))
    same = mg["frames"] == mc["frames"]
    print("manifest frames[] identical:", same, "(%d frames)" % len(mg["frames"]))
    rc |= not same
    for f in ("tx_samples.cf32", "rx_stimulus.cf32"):
        if not (os.path.exists(g + "/" + f) and os.path.exists(c + "/" + f)):
            print(f, "skipped (missing on one side)")
            continue
        a = np.fromfile(g + "/" + f, dtype=np.complex64)
        b = np.fromfile(c + "/" + f, dtype=np.complex64)
        if len(a) != len(b):
            print(f, "LENGTH DIFFERS", len(a), len(b))
            rc = 1
            continue
        d = np.abs(a - b)
        nmse = 10 * np.log10((d ** 2).sum() / (np.abs(a) ** 2).sum()) if (np.abs(a) ** 2).sum() > 0 else float("nan")
        ok = d.max() <= 1e-5
        print("%s: %d samples, max|d| %.3g, NMSE %.1f dB, bit-identical %.1f%% -> %s" % (f, len(a), d.max(), nmse, 100.0 * (d == 0).mean(), "OK" if ok else "OVER 1e-5"))
        rc |= not ok
    return rc


if __name__ == "__main__":
    sys.exit(main())
