"""The unit suite refuses the network (core#1599).

A test that opens a non-local socket or spawns a network CLI fails here, including
when the code under test catches ``Exception``. The class this guards is a unit
test reaching the live GitHub API and staying green because the call happened
to exit with the code the test expected.

IP-literal resolution stays open. ``egress_guard`` classifies a literal itself,
and the DNS stub under ``tests/test_services`` forwards literals to the real
resolver on purpose. A public hostname is a packet, and it is refused.
The RFC 2606 ``.invalid`` TLD is the exception: it is defined not to resolve,
and the unguarded pinning control has to observe that resolver failure.
A connect to any address that comes back is still refused.

``OPT_IN`` is the whole allow-list. Each row names the real service that test
talks to. Connector smoke is skipped unless ``DATANIKA_CONNECTOR_SMOKE=1``;
these rows are what lets that run reach the services it exists to reach.
"""

from __future__ import annotations

import ipaddress
import os
import shlex
import socket
import subprocess
import sys
import threading

REFUSAL = "unit suite refuses the network"

_LOCAL_NAMES = frozenset({"localhost", "localhost.localdomain", "", "none"})
_NET_CLI = frozenset({"gh", "curl", "wget", "ssh", "scp"})

_installed = False
_allowed = False
_allow_lock = threading.Lock()
_real_connect = socket.socket.connect
_real_connect_ex = socket.socket.connect_ex
_real_create = socket.create_connection
_real_gai = socket.getaddrinfo
_real_popen = subprocess.Popen
_real_run = subprocess.run


class UnitSuiteRefusedNetwork(BaseException):
    """Not an Exception: ``except Exception`` must not turn a live call into a pass."""


def allow_network(enabled: bool) -> None:
    global _allowed
    with _allow_lock:
        _allowed = enabled


def network_is_allowed() -> bool:
    with _allow_lock:
        return _allowed


def _refuse(detail: str) -> None:
    if network_is_allowed():
        return
    raise UnitSuiteRefusedNetwork(f"{REFUSAL}: {detail}")


def _host_text(host) -> str:
    if host is None:
        return ""
    if isinstance(host, bytes):
        return host.decode("utf-8", "replace")
    return str(host)


def _ip_literal(host: str):
    try:
        return ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return None


def _loopback_endpoint(host) -> bool:
    text = _host_text(host).strip().lower()
    if text in _LOCAL_NAMES:
        return True
    ip = _ip_literal(text)
    if ip is None:
        return False
    return bool(ip.is_loopback or ip.is_unspecified)


def _endpoint_host(address):
    """Host of a TCP address. ``None`` means this is not a network endpoint."""
    if isinstance(address, tuple) and address:
        return address[0]
    return None


def _connect(self, address):
    host = _endpoint_host(address)
    if host is not None and not _loopback_endpoint(host):
        _refuse(f"connect {address!r}")
    return _real_connect(self, address)


def _connect_ex(self, address):
    host = _endpoint_host(address)
    if host is not None and not _loopback_endpoint(host):
        _refuse(f"connect_ex {address!r}")
    return _real_connect_ex(self, address)


def _create(address, *args, **kwargs):
    host = _endpoint_host(address)
    if host is not None and not _loopback_endpoint(host):
        _refuse(f"create_connection {address!r}")
    return _real_create(address, *args, **kwargs)


def _reserved_invalid(host: str) -> bool:
    """RFC 2606. ``rebind.invalid`` has to reach the real resolver so the failure is an OSError."""
    name = host.strip().rstrip(".").lower()
    return name == "invalid" or name.endswith(".invalid")


def _gai(host, *args, **kwargs):
    text = _host_text(host)
    if _loopback_endpoint(text) or _ip_literal(text) is not None or _reserved_invalid(text):
        return _real_gai(host, *args, **kwargs)
    _refuse(f"getaddrinfo {host!r}")
    return _real_gai(host, *args, **kwargs)


def _basename(token: str) -> str:
    name = token.replace("\\", "/").rsplit("/", 1)[-1].lower()
    if name.endswith(".exe"):
        name = name[:-4]
    return name


def _cli_token(args):
    if isinstance(args, (str, bytes)):
        text = args.decode() if isinstance(args, bytes) else args
        parts = shlex.split(text, posix=False)
        if not parts:
            return None
        return _basename(parts[0])
    if isinstance(args, (list, tuple)) and args:
        return _basename(str(args[0]))
    return None


def _note(args) -> None:
    base = _cli_token(args)
    if base in _NET_CLI:
        _refuse(f"spawn {base}")


class _Popen(_real_popen):  # type: ignore[misc, valid-type]
    def __init__(self, args, *args_, **kwargs):
        _note(args)
        super().__init__(args, *args_, **kwargs)


def _run(args, *args_, **kwargs):
    _note(args)
    return _real_run(args, *args_, **kwargs)


def _stop_incidental_probes() -> None:
    """The libraries read these before they open a socket.

    ``NO_GCE_CHECK`` is cached at import of ``google.auth.compute_engine._metadata``,
    so an environment set after that import does nothing until the cached flag
    moves. botocore reads ``AWS_EC2_METADATA_DISABLED`` when it builds the
    fetcher, and dbt forces anonymous stats off when ``DO_NOT_TRACK`` is set.
    The connect guard still refuses 169.254.169.254, and the lookup guard still
    refuses the telemetry host.
    """
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    os.environ["NO_GCE_CHECK"] = "true"
    os.environ["DO_NOT_TRACK"] = "1"
    meta = sys.modules.get("google.auth.compute_engine._metadata")
    if meta is not None:
        meta._NO_GCE_CHECK = True


def install() -> None:
    global _installed
    _stop_incidental_probes()
    if _installed:
        return
    _installed = True
    socket.socket.connect = _connect  # type: ignore[method-assign]
    socket.socket.connect_ex = _connect_ex  # type: ignore[method-assign]
    socket.create_connection = _create
    socket.getaddrinfo = _gai
    subprocess.Popen = _Popen  # type: ignore[misc]
    subprocess.run = _run


# nodeid without a parametrize suffix -> the real service that test calls.
OPT_IN: dict[str, str] = {
    "tests/test_connector_smoke/test_google_sheets_rows.py"
    "::test_google_sheets_moves_rows_into_the_destination": "Google Sheets",
    "tests/test_connector_smoke/test_paid_connectors.py"
    "::test_bigquery_auth_and_list_datasets": "BigQuery",
    "tests/test_connector_smoke/test_paid_connectors.py"
    "::test_databricks_auth_and_list_uc_catalogs": "Databricks",
    "tests/test_connector_smoke/test_paid_connectors.py"
    "::test_databricks_auth_and_list_warehouses": "Databricks",
    "tests/test_connector_smoke/test_paid_connectors.py"
    "::test_ga4_auth_and_run_report": "Google Analytics 4",
    "tests/test_connector_smoke/test_paid_connectors.py"
    "::test_ga4_property_metadata_introspectable": "Google Analytics 4",
    "tests/test_connector_smoke/test_paid_connectors.py"
    "::test_hubspot_auth_and_account_details": "HubSpot",
    "tests/test_connector_smoke/test_paid_connectors.py"
    "::test_hubspot_contacts_schema_introspectable": "HubSpot",
    "tests/test_connector_smoke/test_paid_connectors.py"
    "::test_kafka_auth_and_list_topics": "Redpanda",
    "tests/test_connector_smoke/test_paid_connectors.py"
    "::test_shopify_auth_and_shop_metadata": "Shopify",
    "tests/test_connector_smoke/test_paid_connectors.py"
    "::test_shopify_read_scopes_cover_core_resources": "Shopify",
    "tests/test_connector_smoke/test_paid_connectors.py"
    "::test_stripe_auth_and_livemode_guard": "Stripe",
    "tests/test_connector_smoke/test_paid_connectors.py"
    "::test_stripe_list_charges_read_scope": "Stripe",
    "tests/test_connector_smoke/test_wave1_connectors_extract_load.py"
    "::test_asana_extract_load_assert": "Asana",
    "tests/test_connector_smoke/test_wave1_connectors_extract_load.py"
    "::test_asana_tasks_load_is_non_empty": "Asana",
    "tests/test_connector_smoke/test_wave1_connectors_extract_load.py"
    "::test_freshdesk_extract_load_assert": "Freshdesk",
    "tests/test_connector_smoke/test_wave1_connectors_extract_load.py"
    "::test_oracle_extract_load_assert": "Oracle",
    "tests/test_connector_smoke/test_wave1_connectors_extract_load.py"
    "::test_pipedrive_extract_load_assert": "Pipedrive",
    "tests/test_connector_smoke/test_wave1_connectors_smoke.py"
    "::test_asana_auth_and_current_user": "Asana",
    "tests/test_connector_smoke/test_wave1_connectors_smoke.py"
    "::test_freshdesk_auth_and_current_agent": "Freshdesk",
    "tests/test_connector_smoke/test_wave1_connectors_smoke.py"
    "::test_oracle_live_driver_and_connector": "Oracle",
    "tests/test_connector_smoke/test_wave1_connectors_smoke.py"
    "::test_pipedrive_auth_and_current_user": "Pipedrive",
}


def node_key(nodeid: str) -> str:
    return nodeid.split("[", 1)[0].replace("\\", "/")


def mark_opt_ins(items) -> None:
    import pytest

    for item in items:
        service = OPT_IN.get(node_key(item.nodeid))
        if service is None:
            continue
        item.add_marker(pytest.mark.needs_network(service=service))


def service_for(item) -> str | None:
    marker = item.get_closest_marker("needs_network")
    if marker is None:
        return None
    service = marker.kwargs.get("service")
    if not isinstance(service, str) or not service.strip():
        raise UnitSuiteRefusedNetwork(
            f"{REFUSAL}: needs_network requires service=<the real service this test calls>"
        )
    return service
