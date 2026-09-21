"""`up` must refuse a port the OS will not let it bind, BEFORE it starts anything (core#1481).

## What happened

`worktree-stack.sh up` exited 1 with **four of six services running**. `app` could not publish
host port `53000`, and `proxy` depends on it, so the stack was left half-started behind:

    Error response from daemon: ports are not available: exposing port TCP 127.0.0.1:53000 ...
    bind: An attempt was made to access a socket in a way forbidden by its access permissions.

Cause, measured at OS level: Product's band frontend ports (53000, 53001, 53010) fall inside a
**Windows reserved TCP range**, 52963-53062. `netsh interface ipv4 show excludedportrange
protocol=tcp` lists it. The ranges are WinNAT's and **move between reboots**, so this appears and
disappears without anyone changing the repository.

`--port-offset` cannot route around it: it takes a multiple of 10000, i.e. it selects *another
department's whole band*, which defeats the isolation this script exists to provide.

## Why the two error numbers are the design

A refused bind is two completely different conditions that docker reports with one sentence:

* **`EACCES`** -- the OS refuses the port outright. It will never work; starting the stack is
  guaranteed to fail partway. **This is a problem, and `up` must refuse before touching anything.**
* **`EADDRINUSE`** -- something already holds it, **very often this stack, already up**. Refusing
  here would make a live stack impossible to re-`up`, which is worse than the defect being fixed.
  **Deliberately not a problem.**

That distinction is the whole reason this is a separate check rather than "try it and see".

## Why the probe is injected

A test that binds real sockets would assert nothing on a CI runner where every port is free -- the
EACCES arm would never execute, and the guard would be green everywhere and unable to fail on the
one machine it exists for. The probe is a parameter, so each arm names the condition it is testing
and runs identically on every OS. `TestTheProbeItself` then checks the real binder against real
sockets, which is the part that cannot be faked.
"""

from __future__ import annotations

import errno
import importlib.util
import socket
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "scripts" / "worktree_stack.py"

_spec = importlib.util.spec_from_file_location("worktree_stack", MODULE)
assert _spec and _spec.loader, f"cannot load {MODULE}"
ws = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ws)


def _port(published: int, target: int, host_ip: str = "127.0.0.1") -> dict:
    return {
        "mode": "ingress",
        "host_ip": host_ip,
        "target": target,
        "published": str(published),
        "protocol": "tcp",
    }


RENDERED = {
    "name": "wt-product",
    "services": {
        "app": {
            "container_name": "wt-product-app",
            "ports": [_port(53000, 3000), _port(58000, 8000)],
        },
        "postgres": {
            "container_name": "wt-product-postgres",
            "ports": [_port(55432, 5432)],
        },
        "celery": {"container_name": "wt-product-celery"},
    },
}


def probe_returning(mapping: dict[int, int | None]):
    """A stand-in binder: port -> errno, or None for 'bound fine'."""

    def probe(host_ip: str, port: int) -> int | None:
        return mapping.get(port)

    return probe


class TestAReservedPortIsRefusedBeforeAnythingStarts:
    def test_an_eacces_port_is_a_problem(self):
        problems = ws.unbindable(RENDERED, probe=probe_returning({53000: errno.EACCES}))
        assert len(problems) == 1, problems
        assert "53000" in problems[0]
        assert "app" in problems[0]

    def test_the_problem_says_where_to_look_rather_than_only_that_it_failed(self):
        """A refusal a reader cannot act on costs the same round the defect did.

        Asserts the PRESENCE of the remedy, not the absence of a word: a message that
        merely dropped the word 'reserved' would still pass a ban, and would still be
        useless.
        """
        problems = ws.unbindable(RENDERED, probe=probe_returning({53000: errno.EACCES}))
        assert "excludedportrange" in problems[0], (
            "the message must name the command that lists the ranges, or the reader is "
            "left with a bind failure and no next step"
        )

    def test_every_reserved_port_is_named_not_just_the_first(self):
        """Docker reports only the port it tripped on first, which is what made this take
        two passes to diagnose. The pre-flight has no reason to stop early."""
        problems = ws.unbindable(
            RENDERED,
            probe=probe_returning({53000: errno.EACCES, 55432: errno.EACCES}),
        )
        assert len(problems) == 2, problems
        assert {"53000", "55432"} <= {p.split()[-1] for p in problems} or all(
            any(port in p for p in problems) for port in ("53000", "55432")
        )


class TestWhatMustNotBeRefused:
    def test_a_port_already_in_use_is_not_a_problem(self):
        """The control that decides the whole design.

        EADDRINUSE is what a LIVE stack's own ports return. If this were a problem, a
        running stack could never be re-`up`ed, and the fix would be worse than the bug.
        """
        assert ws.unbindable(RENDERED, probe=probe_returning({53000: errno.EADDRINUSE})) == []

    def test_the_windows_in_use_errno_is_also_not_a_problem(self):
        """On Windows the value observed is 10048 (WSAEADDRINUSE), and `errno.EADDRINUSE`
        is the POSIX 100 -- so comparing against the constant alone silently misses it."""
        assert ws.unbindable(RENDERED, probe=probe_returning({53000: 10048})) == []

    def test_all_ports_bindable_is_no_problems(self):
        assert ws.unbindable(RENDERED, probe=probe_returning({})) == []

    def test_a_service_with_no_published_ports_is_skipped(self):
        """`celery` publishes nothing; a probe that were called for it would be asking
        about a port that does not exist."""
        asked: list[int] = []

        def probe(host_ip: str, port: int) -> int | None:
            asked.append(port)
            return None

        ws.unbindable(RENDERED, probe=probe)
        assert sorted(asked) == [53000, 55432, 58000]


class TestAnUnrecognisedConditionFailsClosed:
    def test_an_unknown_errno_is_a_problem_and_names_itself(self):
        """Fail closed: an unknown refusal is reported rather than assumed harmless, and
        the errno is printed so it is diagnosable rather than mysterious."""
        problems = ws.unbindable(RENDERED, probe=probe_returning({53000: 4242}))
        assert len(problems) == 1, problems
        assert "4242" in problems[0]


class TestTheProbeItself:
    """The injected probe is only honest if the real one behaves as the arms assume."""

    def test_a_free_port_probes_as_bindable(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        free = s.getsockname()[1]
        s.close()
        assert ws.probe_bind("127.0.0.1", free) is None

    def test_a_port_this_process_holds_probes_as_in_use(self):
        """A positive control on the real binder: it must report SOMETHING for a port that
        is genuinely taken, or every reading it gives is meaningless."""
        holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        holder.bind(("127.0.0.1", 0))
        holder.listen(1)
        taken = holder.getsockname()[1]
        try:
            result = ws.probe_bind("127.0.0.1", taken)
            assert result is not None, "the real probe cannot see a port it is holding itself"
            assert result in ws.IN_USE_ERRNOS, (
                f"a self-held port reported errno {result}, which is not in IN_USE_ERRNOS -- "
                "so `unbindable` would call a live stack's own port a reserved one and refuse "
                "every re-up"
            )
        finally:
            holder.close()


class TestTheCheckActuallyRunsIt:
    """A pre-flight nothing calls is this defect one level up, and looks identical to a fix."""

    def test_main_refuses_when_a_port_is_reserved(self, monkeypatch, capsys):
        # The seam under test is "does `main` fold `unbindable` into its refusal", so the
        # document is injected at `_stdin_json` rather than through real stdin plumbing —
        # which is a different concern and already covered by the CLI's own tests.
        monkeypatch.setattr(ws, "_stdin_json", lambda: ISOLATED_PRODUCT)
        monkeypatch.setattr(ws, "probe_bind", lambda host_ip, port: errno.EACCES)

        code = ws.main(["check", "--agent", "product", "--offset", "50000"])

        assert code == 1, "main returned 0 with a port the OS refuses, so `up` would proceed"
        assert "53000" in capsys.readouterr().err

    def test_main_accepts_the_same_rendering_when_the_ports_bind(self, monkeypatch):
        """The control, and it is what makes the arm above mean something.

        Same document, same code path, only the probe changes. Without it, a `main` that
        refused everything — or a rendering that was not isolated for some unrelated
        reason — would produce the same exit code and look like a working guard.
        """
        monkeypatch.setattr(ws, "_stdin_json", lambda: ISOLATED_PRODUCT)
        monkeypatch.setattr(ws, "probe_bind", lambda host_ip, port: None)

        assert ws.main(["check", "--agent", "product", "--offset", "50000"]) == 0


#: A fully isolated product rendering: correct project, container names, band, host_ip,
#: REFLEX_API_URL and image. Nothing in `check` can object to it, so a refusal can only
#: come from the port pre-flight.
ISOLATED_PRODUCT = {
    "name": "wt-product",
    "services": {
        "app": {
            "container_name": "wt-product-app",
            "image": f"{ws.IMAGE_REPO}:worktree-product",
            "ports": [_port(53000, 3000), _port(58000, 8000)],
            "environment": {"REFLEX_API_URL": "http://localhost:58000"},
        },
        "postgres": {
            "container_name": "wt-product-postgres",
            "image": "postgres:16-alpine",
            "ports": [_port(55432, 5432)],
        },
    },
}
