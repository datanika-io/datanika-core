"""A ClickHouse destination loads through the port the connection form stores (core#1341).

The ClickHouse connection form has one port field, labelled **"HTTP port"** and pre-filled with
8123, plus a *Use HTTPS (TLS)* checkbox. Test Connection (``_build_sa_url``) and dbt
(``dbt_project.py``) both use that value as the HTTP port. dlt's ClickHouse destination needs
**two** ports:

* ``port``: the native TCP port its ``sync`` step dials through clickhouse-driver.
* ``http_port``: the HTTP interface its file load uses through clickhouse-connect.

``_to_dlt_credentials`` handed the stored ``port`` to dlt as the native port and set no
``http_port``. dlt then used its own default of 8443, and port 8443 alone switches
clickhouse-connect to TLS. QA measured the result on a stock server:

* The form's default port failed at ``sync`` with ``EOFError: Unexpected EOF while reading bytes``.
* A native port typed into the field got past ``sync`` and created the tables, then failed the load
  against 8443.

**The witness is a real ClickHouse load** through ``DltRunnerService.execute()``, using the config
the form's own ``_build_config`` stores. Test Connection cannot be the witness: it speaks HTTP, and
it passed while every load failed.
"""

from __future__ import annotations

import contextlib
import copy
import socket

from datanika.services.dlt_runner import DltRunnerService
from datanika.ui.state.connection_state import _DEFAULT_PORTS, ConnectionState
from tests.test_services.test_destination_credential_contract import (
    _declared_credential_fields,
)
from tests.test_services.test_rerun_lands_each_record_once import (
    _field_default,
    _write_csv,
    form_config,
)
from tests.test_services.test_source_builders_move_rows import requires_docker

#: The server version QA measured #1341 against.
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:24.8.14.39"
USER = "probe"
PASSWORD = "probe-password"
DATABASE = "probe_db"
#: A non-default HTTP port, standing in for a user editing the field.
EDITED_HTTP_PORT = 18123


class _ConnectionForm:
    """A stand-in `self` with ConnectionState's declared vars, so the REAL `_build_config` runs."""

    _build_config = ConnectionState._build_config

    def __init__(self, **values):
        for name, field in ConnectionState.get_fields().items():
            setattr(self, name, _field_default(field))
        # What `set_form_type` sets when ClickHouse is picked.
        self.form_type = "clickhouse"
        self.form_port = _DEFAULT_PORTS["clickhouse"]
        for name, value in values.items():
            setattr(self, name, value)


def stored_connection(host: str = "h", **values) -> dict:
    """The config the ClickHouse connection form stores."""
    fields = {
        "form_host": host,
        "form_user": USER,
        "form_password": PASSWORD,
        "form_database": DATABASE,
        **values,
    }
    return _ConnectionForm(**fields)._build_config()


def _destination_credentials(connection: dict) -> dict:
    destination = DltRunnerService().build_destination("clickhouse", copy.deepcopy(connection))
    return destination.config_params["credentials"]


class TestTheFormsPortReachesDltAsItsHttpPort:
    def test_the_harness_stores_the_forms_default_port(self):
        """Anti-vacuity: the arms below test the form's 8123, not a number this file chose."""
        assert stored_connection()["port"] == 8123
        assert stored_connection()["secure"] is False

    def test_the_default_port_is_sent_as_http_port_beside_the_native_default(self):
        creds = _destination_credentials(stored_connection())

        assert (creds.get("http_port"), creds.get("port")) == (8123, 9000), (
            "the form's HTTP port must reach dlt as `http_port`, with ClickHouse's native default "
            f"as `port`; dlt was handed {creds}"
        )

    def test_an_edited_port_is_sent_as_http_port(self):
        creds = _destination_credentials(stored_connection(form_port=str(EDITED_HTTP_PORT)))

        assert (creds.get("http_port"), creds.get("port")) == (EDITED_HTTP_PORT, 9000), creds

    def test_a_tls_connection_derives_the_tls_native_port(self):
        """ClickHouse Cloud and TLS servers: HTTPS 8443 beside native TLS 9440."""
        creds = _destination_credentials(stored_connection(form_port="8443", form_secure=True))

        assert (creds.get("http_port"), creds.get("port")) == (8443, 9440), creds
        assert creds.get("secure") is True

    def test_every_key_sent_is_one_dlt_declares(self):
        produced = set(_destination_credentials(stored_connection(form_secure=True)))

        assert not produced - _declared_credential_fields("clickhouse"), sorted(produced)

    def test_a_config_stating_http_port_in_dlts_own_terms_is_left_as_given(self):
        """A raw-JSON connection may already name both ports the way dlt's documentation does.
        Rewriting its `port` would point dlt's HTTP client at the native port."""
        connection = {**stored_connection(), "port": 9000, "http_port": 8123}

        creds = _destination_credentials(connection)

        assert (creds["http_port"], creds["port"]) == (8123, 9000), creds


class TestTheSharedTranslationKeepsTheHttpPort:
    def test_control_a_clickhouse_source_still_gets_the_stored_port(self):
        """ClickHouse is also a SOURCE, through `clickhousedb+connect`, which speaks HTTP.

        `_to_dlt_credentials` serves both directions, so the destination's mapping must not live
        in it. Moving it there would send a ClickHouse source to the native port.
        """
        creds = DltRunnerService._to_dlt_credentials("clickhouse", stored_connection())

        assert creds["drivername"] == "clickhousedb+connect"
        assert creds["port"] == 8123
        assert "http_port" not in creds


@contextlib.contextmanager
def _clickhouse_serving_http_on(host_http_port: int):
    """A real ClickHouse whose HTTP interface is on ``host_http_port`` and native port on 9000.

    Fixed host ports, on purpose. The native port is DERIVED, not stored: without TLS it is 9000,
    so the server's native port has to be where the derivation says. A random mapping would move
    it, and the load would fail for the harness's reason, not the product's.

    One server per arm, so the edited-port arm runs with NOTHING on 8123. A loader that ignored
    the stored port and dialled the default could not reach its server.
    """
    from testcontainers.clickhouse import ClickHouseContainer

    container = ClickHouseContainer(
        CLICKHOUSE_IMAGE, username=USER, password=PASSWORD, dbname=DATABASE
    )
    container.with_bind_ports(9000, 9000)
    container.with_bind_ports(8123, host_http_port)
    with container:
        yield container


def _nothing_listens_on(port: int) -> bool:
    with socket.socket() as probe:
        probe.settimeout(1)
        return probe.connect_ex(("127.0.0.1", port)) != 0


def _load(tmp_path, connection: dict, dataset: str) -> None:
    drop = tmp_path / "drop"
    drop.mkdir()
    _write_csv(drop)
    DltRunnerService(pipelines_dir=str(tmp_path / "dlt")).execute(
        pipeline_id=7,
        source_type="csv",
        source_config={"bucket_url": str(drop)},
        destination_type="clickhouse",
        destination_config=copy.deepcopy(connection),
        dlt_config=form_config("csv", form_file_glob="widgets.csv"),
        dataset_name=dataset,
        run_id=1,
    )


def _rows(clickhouse, dataset: str) -> tuple[int, int]:
    """(rows, distinct ids) in the loaded table, read from ClickHouse itself."""
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=clickhouse.get_container_host_ip(),
        port=int(clickhouse.get_exposed_port(8123)),
        username=USER,
        password=PASSWORD,
    )
    try:
        result = client.query(f"SELECT count(), uniqExact(id) FROM {DATABASE}.{dataset}___widgets")
        rows, distinct = result.result_rows[0]
    finally:
        client.close()
    return int(rows), int(distinct)


@requires_docker
class TestARealClickHouseLoad:
    def test_the_forms_default_port_loads(self, tmp_path):
        with _clickhouse_serving_http_on(8123) as clickhouse:
            connection = stored_connection(clickhouse.get_container_host_ip())
            assert connection["port"] == 8123

            _load(tmp_path, connection, "form_default")

            assert _rows(clickhouse, "form_default") == (3, 3)

    def test_an_edited_http_port_loads(self, tmp_path):
        with _clickhouse_serving_http_on(EDITED_HTTP_PORT) as clickhouse:
            assert _nothing_listens_on(8123), "this arm must run with nothing on the default port"
            connection = stored_connection(
                clickhouse.get_container_host_ip(), form_port=str(EDITED_HTTP_PORT)
            )

            _load(tmp_path, connection, "edited_port")

            assert _rows(clickhouse, "edited_port") == (3, 3)

    def test_control_the_server_takes_a_load_given_both_ports(self, tmp_path):
        """QA's control arm. It loads before and after the fix, so a red above is the port,
        not this server or this harness."""
        with _clickhouse_serving_http_on(8123) as clickhouse:
            connection = {
                **stored_connection(clickhouse.get_container_host_ip()),
                "port": 9000,
                "http_port": 8123,
            }

            _load(tmp_path, connection, "both_ports")

            assert _rows(clickhouse, "both_ports") == (3, 3)
