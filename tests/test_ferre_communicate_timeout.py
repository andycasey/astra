"""
Two-phase communication timeout for `astra.pipelines.aspcap.ferre`.

FERRE is legitimately silent in two situations, and neither means it has stopped working:

  * while loading its grid (tens of GB, no output during the read), and
  * between dispatch and the first completed object -- with `NOBJ >= NTHREADS` every thread is
    handed an object at once and they finish in a burst, so nothing is reported until roughly the
    whole first wave lands.

`max_t_communicate` was measuring exactly that window, which made it a per-object *latency* budget
rather than a liveness check. Measured against a real 28 GB grid, first-completion latency was ~917s
at NTHREADS=128 versus a 1000s budget -- and production runs, with colder caches, lost entire grids
to it (128 objects dispatched, zero completed, killed, everything written as NaN).

Once results start flowing the picture inverts: completions and dispatches interleave continuously
and the largest observed gap drops to ~182s, so a long silence there really is a problem.

Hence a two-phase budget: `max_t_communicate_first_result` until a (sub-)execution produces its
first result, `max_t_communicate` afterwards.

The distinction matters most in list (`-l`) mode, used by the abundances stage. Every entry reloads
the grid and has its own silent first wave, but `t_overhead` is only ever set once -- so a gate
based on it, or on a cumulative completion count, protects only the first element. In a real 0.9.4
run this killed all 21 elements after the first, one per execution, each costing a full grid reload.
"""

import os

os.environ["ASTRA_DATABASE_PATH"] = ":memory:"

import tempfile
import threading
from unittest import mock

import pytest


# Verbatim from real FERRE stdout; the banner is printed once per execution, and once per entry in
# list mode. "Done reading!" marks the end of a grid load.
FERRE_BANNER = "                     f e r r e          v4.8.10     \n"
DONE_READING = " Done reading!\n"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class RecordingPipe:
    def __init__(self):
        self.messages = []

    def send(self, message):
        self.messages.append(message)


# How long a stalled fake FERRE stays silent before giving up and reporting EOF. Must sit between
# the "tight" and "generous" budgets used below, so that a watchdog that should fire has time to,
# and one that should not simply reaches the end of the stall.
STALL_SECONDS = 8


class _FakeStream:
    """Emits lines, then optionally goes silent to simulate FERRE working without reporting."""

    def __init__(self, lines, stall_after=None, kill_event=None):
        self._lines = list(lines)
        self._i = 0
        self._stall_after = stall_after
        self._kill_event = kill_event

    def readline(self):
        if self._stall_after is not None and self._i >= self._stall_after:
            # Returns early if the watchdog kills us, otherwise sits silent for the full stall.
            self._kill_event.wait(timeout=STALL_SECONDS)
            return ""
        if self._i < len(self._lines):
            line = self._lines[self._i]
            self._i += 1
            return line
        return ""

    def read(self):
        return ""

    def close(self):
        pass


class _FakeProcess:
    def __init__(self, lines, stall_after=None):
        self.kill_event = threading.Event()
        self.stdout = _FakeStream(lines, stall_after, self.kill_event)
        self.stderr = _FakeStream([])
        self.was_killed = False
        self._returncode = 0

    def kill(self):
        self.was_killed = True
        self._returncode = -9
        self.kill_event.set()

    def wait(self):
        return self._returncode


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------

def run_ferre(
    lines, stall_after, max_t_communicate, max_t_communicate_first_result,
    max_t_elapsed=None, max_sigma_outlier=None,
):
    """
    Drive ferre() against a faked subprocess; returns (result, elapsed_seconds).

    `result` is a namespace with two independent flags, since the "no communication" and
    per-object "sigma outlier" checks are separate watchdogs that can each fire on their own:
      * result.communicate_fired -- the max_t_communicate[_first_result] check
      * result.outlier_fired     -- the max_t_elapsed / max_sigma_outlier check

    Keyed off the watchdogs' own log lines rather than whether the process object saw a kill():
    ferre() calls kill() unconditionally as cleanup once the reader loop ends, so a clean run and a
    watchdog kill are indistinguishable from the process object alone.
    """
    from time import time as _time
    from types import SimpleNamespace
    from astra.pipelines import aspcap

    process = _FakeProcess(lines, stall_after)
    logged = []

    with tempfile.TemporaryDirectory() as directory:
        with open(os.path.join(directory, "parameter.input"), "w") as fp:
            for i in range(10):
                fp.write(f"{i}_100{i}_200{i}_0_None   0.0 0.0 0.0 0.0 0.0 0.0 0.0 5000.0\n")
        input_nml_path = os.path.join(directory, "input.nml")
        open(input_nml_path, "w").close()

        started = _time()
        with mock.patch.object(aspcap.subprocess, "Popen", return_value=process), \
             mock.patch.object(aspcap, "re_process_partial_ferre", return_value=(None, None)), \
             mock.patch.object(aspcap, "merge_partial_ferre_outputs", return_value=None), \
             mock.patch.object(aspcap, "debugger",
                               lambda *a, **k: logged.append(" ".join(map(str, a)))):
            aspcap.ferre(
                input_nml_path,
                directory,
                n_obj=10,
                n_threads=8,
                pipe=RecordingPipe(),
                communicate_on_start=True,
                max_t_communicate=max_t_communicate,
                max_t_communicate_first_result=max_t_communicate_first_result,
                max_t_elapsed=max_t_elapsed,
                max_sigma_outlier=max_sigma_outlier,
                max_t_grid_load=None,
                max_resume_attempts=0,
            )

    assert not any("MONITOR DIED" in m for m in logged), "the monitor thread raised"
    result = SimpleNamespace(
        communicate_fired=any("hanging no communication" in m for m in logged),
        # The sigma-outlier kill logs "hanging [<indices>]" -- distinct from the
        # "hanging on <path>" summary line emitted afterwards regardless of cause.
        outlier_fired=any(m.startswith("hanging [") for m in logged),
    )
    return result, _time() - started


# ---------------------------------------------------------------------------
# The marker regex
# ---------------------------------------------------------------------------

def test_execution_start_regex_matches_banner_and_load_end():
    from astra.pipelines.aspcap import REGEX_EXECUTION_START
    assert REGEX_EXECUTION_START.search(FERRE_BANNER)
    assert REGEX_EXECUTION_START.search(DONE_READING)


def test_execution_start_regex_ignores_dispatch_and_completion_lines():
    """Must not fire on ordinary progress, or the first-result budget would never expire."""
    from astra.pipelines.aspcap import REGEX_EXECUTION_START
    assert not REGEX_EXECUTION_START.search(" next object #         1\n")
    assert not REGEX_EXECUTION_START.search("           1 0_1000_2000_0_None\n")
    assert not REGEX_EXECUTION_START.search(" median snr =   119.737000\n")


# ---------------------------------------------------------------------------
# Before the first result: the generous budget applies
# ---------------------------------------------------------------------------

def test_silence_before_first_result_is_not_killed_at_the_steady_state_budget():
    """
    The regression test. Startup, grid load, dispatch, then silence -- the shape that lost whole
    grids in production. With a steady-state budget of 2s and a first-result budget of 60s, the
    process must survive: it has not reported a result yet, so this silence is expected.
    """
    lines = [
        FERRE_BANNER,
        DONE_READING,
        " next object #         1\n",
        " next object #         2\n",
    ]
    result, elapsed = run_ferre(
        lines, stall_after=4, max_t_communicate=2, max_t_communicate_first_result=60
    )
    assert not result.communicate_fired, (
        "FERRE was killed during the pre-first-result window; that silence is expected and should "
        "be governed by max_t_communicate_first_result, not max_t_communicate"
    )
    # Sanity: it really did sit silent well past the steady-state budget rather than exiting early.
    assert elapsed > 4, f"expected a real stall of >4s, only took {elapsed:.1f}s"


def test_silence_before_first_result_is_killed_once_its_own_budget_expires():
    """The generous budget is a longer leash, not an absence of one."""
    lines = [
        FERRE_BANNER,
        DONE_READING,
        " next object #         1\n",
    ]
    result, _ = run_ferre(
        lines, stall_after=3, max_t_communicate=60, max_t_communicate_first_result=2
    )
    assert result.communicate_fired, "a stall exceeding max_t_communicate_first_result must still be caught"


# ---------------------------------------------------------------------------
# After the first result: the tight budget applies
# ---------------------------------------------------------------------------

def test_silence_after_first_result_is_killed_at_the_steady_state_budget():
    """
    Once a result has been reported, completions and dispatches interleave continuously, so a long
    gap is genuinely suspicious and must be caught promptly -- not held to the generous budget.
    """
    lines = [
        FERRE_BANNER,
        DONE_READING,
        " next object #         1\n",
        "           1 0_1000_2000_0_None\n",   # first result -> tighten the budget
        " next object #         2\n",
    ]
    result, _ = run_ferre(
        lines, stall_after=5, max_t_communicate=2, max_t_communicate_first_result=60
    )
    assert result.communicate_fired, (
        "after a result was reported the steady-state budget applies; this stall should have been "
        "caught at max_t_communicate rather than waiting for max_t_communicate_first_result"
    )


# ---------------------------------------------------------------------------
# List mode: every entry gets its own first-result window
# ---------------------------------------------------------------------------

def test_new_sub_execution_restores_the_first_result_budget():
    """
    In list (-l) mode each entry reloads the grid and has its own silent first wave. After entry one
    completes, a naive "have we ever completed anything?" gate would leave entry two exposed to the
    tight budget during its reload -- which is exactly what killed all 21 elements after the first
    in a real 0.9.4 abundances run, one per execution.
    """
    lines = [
        FERRE_BANNER,
        DONE_READING,
        " next object #         1\n",
        "           1 0_1000_2000_0_None\n",   # entry one produces a result
        FERRE_BANNER,                          # entry two begins: budget must reset
        DONE_READING,
        " next object #         1\n",
    ]
    result, elapsed = run_ferre(
        lines, stall_after=7, max_t_communicate=2, max_t_communicate_first_result=60
    )
    assert not result.communicate_fired, (
        "a new sub-execution must restore the first-result budget; otherwise every list-mode entry "
        "after the first is killed during its own grid load"
    )
    assert elapsed > 4, f"expected a real stall of >4s, only took {elapsed:.1f}s"


# ---------------------------------------------------------------------------
# The per-object outlier watchdog (max_t_elapsed / max_sigma_outlier) has the same flaw
# ---------------------------------------------------------------------------
#
# This is a second, independent watchdog from max_t_communicate: it flags an individual object as
# hung if its wait time exceeds max_t_elapsed, or is an outlier by more than max_sigma_outlier
# standard deviations from the median of *completed* objects. Before any object has completed,
# there is no such median to compare against -- the code falls back to a hardcoded
# median=120.0, stddev=10.0 -- so it is not measuring anything real. In a production run this fired
# repeatedly on a grid whose true first-completion latency was on the order of max_t_elapsed
# itself, flagging ~128 objects as "hanging" and exhausting all 5 resume attempts (each one
# reproducing the same failure on a shrunken batch) while never completing a single object.
#
# The fix mirrors the communication budget: skip this test entirely while awaiting_first_result is
# true, and rely on max_t_communicate_first_result as the sole guard for that window.

def test_outlier_watchdog_does_not_fire_before_first_result():
    """
    The regression test for the second watchdog. Two objects dispatched, neither ever completes.
    max_t_elapsed=1 is tiny enough that the old code kills this almost immediately; with the fix,
    awaiting_first_result stays True (nothing has completed), so this check must not run at all.
    """
    lines = [
        FERRE_BANNER,
        DONE_READING,
        " next object #         1\n",
        " next object #         2\n",
    ]
    result, elapsed = run_ferre(
        lines, stall_after=4,
        max_t_communicate=60, max_t_communicate_first_result=60,
        max_t_elapsed=1, max_sigma_outlier=None,
    )
    assert not result.outlier_fired, (
        "an object waiting past max_t_elapsed before any result was killed; with zero completions "
        "the median/stddev it is compared against is a hardcoded fallback, not measured data, so "
        "this window must be governed by max_t_communicate_first_result instead"
    )
    assert elapsed > 4, f"expected a real stall of >4s, only took {elapsed:.1f}s"


def test_outlier_watchdog_still_fires_after_first_result():
    """A real outlier, once real completion-time data exists, must still be caught promptly."""
    lines = [
        FERRE_BANNER,
        DONE_READING,
        " next object #         1\n",
        "           1 0_1000_2000_0_None\n",   # first result -> real median/stddev now exist
        " next object #         2\n",
    ]
    result, _ = run_ferre(
        lines, stall_after=5,
        max_t_communicate=60, max_t_communicate_first_result=60,
        max_t_elapsed=1, max_sigma_outlier=None,
    )
    assert result.outlier_fired, (
        "once a result has been reported, an object exceeding max_t_elapsed should still be "
        "flagged -- this watchdog must only be suppressed before the first result, not always"
    )
