"""Attribute suite wall time to subprocess children and socket.connect.

DIAGNOSTIC ONLY -- core#961. Not part of the product and not merged to dev. This
exists so the same instrumentation runs on ubuntu-latest and on a Windows dev box,
making the two directly comparable; the local-vs-CI gap cannot be split without a
Linux baseline to subtract.

Deliberately hooks Popen.__init__ SEPARATELY from subprocess.run and never sums
them: run() calls __init__ internally, so summing double-counts every call and
inflates the spawn figure. Only __init__ is process creation; run() wall time is
the child's entire lifetime, which is work both platforms perform.
"""

import atexit
import collections
import os
import socket
import subprocess
import time

_cur = {"id": "<session>"}
_sub_s: collections.Counter = collections.Counter()
_sub_n: collections.Counter = collections.Counter()
_con_s: collections.Counter = collections.Counter()
_con_n: collections.Counter = collections.Counter()
_tot = {"spawn_s": 0.0, "spawn_n": 0, "sub_s": 0.0, "sub_n": 0, "con_s": 0.0, "con_n": 0, "slow": 0}

_orig_init = subprocess.Popen.__init__


def _init(self, *args, **kwargs):
    t0 = time.perf_counter()
    try:
        return _orig_init(self, *args, **kwargs)
    finally:
        _tot["spawn_s"] += time.perf_counter() - t0
        _tot["spawn_n"] += 1


subprocess.Popen.__init__ = _init

_orig_run = subprocess.run


def _run(*args, **kwargs):
    t0 = time.perf_counter()
    try:
        return _orig_run(*args, **kwargs)
    finally:
        dt = time.perf_counter() - t0
        _sub_s[_cur["id"]] += dt
        _sub_n[_cur["id"]] += 1
        _tot["sub_s"] += dt
        _tot["sub_n"] += 1


subprocess.run = _run

_orig_connect = socket.socket.connect


def _connect(self, address, *args, **kwargs):
    t0 = time.perf_counter()
    try:
        return _orig_connect(self, address, *args, **kwargs)
    finally:
        dt = time.perf_counter() - t0
        _con_s[_cur["id"]] += dt
        _con_n[_cur["id"]] += 1
        _tot["con_s"] += dt
        _tot["con_n"] += 1
        if dt > 0.5:
            _tot["slow"] += 1


socket.socket.connect = _connect


def pytest_runtest_protocol(item, nextitem):
    _cur["id"] = item.nodeid
    return None


@atexit.register
def _report():
    print("\n=== PERF ATTRIBUTION (os.name=%s) ===" % os.name)
    print(
        "TOTALS  spawn %.1fs/%d (%.1f ms each)  child %.1fs/%d  connect %.1fs/%d (%d stalls >0.5s)"
        % (
            _tot["spawn_s"], _tot["spawn_n"],
            _tot["spawn_s"] / max(_tot["spawn_n"], 1) * 1000,
            _tot["sub_s"], _tot["sub_n"],
            _tot["con_s"], _tot["con_n"], _tot["slow"],
        )
    )
    print("\n-- top 25 by subprocess-child time --")
    for k, v in _sub_s.most_common(25):
        print("  %8.1fs  %4d calls  %s" % (v, _sub_n[k], k))
    print("\n-- top 15 by connect time --")
    for k, v in _con_s.most_common(15):
        print("  %8.1fs  %4d calls  %s" % (v, _con_n[k], k))
