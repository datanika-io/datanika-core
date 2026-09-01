"""Save-time refusal of destinations that cannot do the job (core#862, core#865).

Three pickers used to offer destinations that could not work, and the failure
arrived **inside a Celery task**, as an untranslated internal string on ``/runs``,
naming a layer the UI never mentions.

🚨 **The cost is quota, not only wording.** ``generate_profiles_yml`` raises
*after* ``run.before_execute`` has fired and after ``start_run`` — so by the time
anything notices, the tenant has been charged for a run that was structurally
incapable of succeeding. On Free that is 1 of 500, and with V2 metering live
against real Paddle it is money.

**Why the refusal is in the SERVICE and not in the picker.** A picker filters
what a browser renders. ``POST /api/v1/pipelines`` is publicly documented at
``datanika.io/api/reference`` and never sees it. The picker is a claim the
client makes; this is the refusal — the same split core#851 established for
destructive controls, and the same reason a disabled button is not a permission
check.

Two capabilities, two sets, and the difference is the whole point:

===========  ===============================  ==================================
type         dlt can LOAD into it?            dbt can TRANSFORM in it?
===========  ===============================  ==================================
postgres     yes                              yes
databricks   **yes**                          **no** — no adapter installed
synapse      **yes**                          **no** — no adapter installed
mysql        no — no dlt factory (core#865)   no — dbt-mysql dropped (core#825)
sqlite       no — no dlt factory (core#865)   no — never had an adapter
===========  ===============================  ==================================

``databricks`` is the discriminating case throughout: it passes
``DESTINATION_TYPES`` and fails ``TRANSFORM_DESTINATION_TYPES``. A test that only
used ``mysql`` would pass against an implementation that had collapsed the two
sets into one, which is the mistake this whole issue is about.
"""

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import ConnectionType
from datanika.models.pipeline import DbtCommand
from datanika.models.transformation import Materialization
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.pipeline_service import PipelineConfigError, PipelineService
from datanika.services.transformation_service import (
    TransformationConfigError,
    TransformationService,
)
from datanika.services.upload_service import UploadService


@pytest.fixture
def encryption():
    return EncryptionService(Fernet.generate_key().decode())


@pytest.fixture
def conn_svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-dest-refusal")
    db_session.add(org)
    db_session.flush()
    return org


def _conn(conn_svc, db_session, org, conn_type: ConnectionType):
    return conn_svc.create_connection(db_session, org.id, conn_type.value, conn_type, {})


class TestPipelinesRefuseANonDbtDestination:
    def test_databricks_is_refused_even_though_it_is_a_load_destination(
        self, conn_svc, db_session, org
    ):
        """The discriminating case. Loadable, not transformable."""
        conn = _conn(conn_svc, db_session, org, ConnectionType.DATABRICKS)
        with pytest.raises(PipelineConfigError, match="no dbt adapter"):
            PipelineService().create_pipeline(
                db_session, org.id, "p", None, conn.id, DbtCommand.RUN
            )

    def test_synapse_is_refused_for_the_same_reason(self, conn_svc, db_session, org):
        conn = _conn(conn_svc, db_session, org, ConnectionType.SYNAPSE)
        with pytest.raises(PipelineConfigError, match="no dbt adapter"):
            PipelineService().create_pipeline(
                db_session, org.id, "p", None, conn.id, DbtCommand.RUN
            )

    def test_postgres_still_works(self, conn_svc, db_session, org):
        """The control. Without it, every assertion above is satisfied by a
        refusal that refuses everything — which is the more likely bug."""
        conn = _conn(conn_svc, db_session, org, ConnectionType.POSTGRES)
        pipeline = PipelineService().create_pipeline(
            db_session, org.id, "p", None, conn.id, DbtCommand.RUN
        )
        assert pipeline.destination_connection_id == conn.id

    def test_update_is_refused_too(self, conn_svc, db_session, org):
        """Create and update take different paths into the same check. A guard
        on create alone leaves the edit form as a way in."""
        good = _conn(conn_svc, db_session, org, ConnectionType.POSTGRES)
        bad = _conn(conn_svc, db_session, org, ConnectionType.DATABRICKS)
        svc = PipelineService()
        pipeline = svc.create_pipeline(db_session, org.id, "p", None, good.id, DbtCommand.RUN)
        with pytest.raises(PipelineConfigError, match="no dbt adapter"):
            svc.update_pipeline(db_session, org.id, pipeline.id, destination_connection_id=bad.id)

    def test_the_message_names_the_type_and_the_reason(self, conn_svc, db_session, org):
        """A refusal the user cannot act on is a different bug.

        Naming the type is safe here and only here: ownership has already been
        established by `get_org_connection`, so this cannot be used to probe
        which connection ids exist in other orgs — which is exactly why the
        *ownership* message above it stays deliberately vague.
        """
        conn = _conn(conn_svc, db_session, org, ConnectionType.DATABRICKS)
        with pytest.raises(PipelineConfigError) as exc:
            PipelineService().create_pipeline(
                db_session, org.id, "p", None, conn.id, DbtCommand.RUN
            )
        assert "databricks" in str(exc.value)
        assert "dbt" in str(exc.value)


class TestTransformationsRefuseTheSame:
    def test_databricks_is_refused(self, conn_svc, db_session, org):
        conn = _conn(conn_svc, db_session, org, ConnectionType.DATABRICKS)
        with pytest.raises(TransformationConfigError, match="no dbt adapter"):
            TransformationService().create_transformation(
                db_session,
                org.id,
                "t",
                "select 1",
                Materialization.VIEW,
                destination_connection_id=conn.id,
            )

    def test_postgres_still_works(self, conn_svc, db_session, org):
        conn = _conn(conn_svc, db_session, org, ConnectionType.POSTGRES)
        t = TransformationService().create_transformation(
            db_session,
            org.id,
            "t_ok",
            "select 1",
            Materialization.VIEW,
            destination_connection_id=conn.id,
        )
        assert t.destination_connection_id == conn.id

    def test_no_destination_is_still_allowed(self, db_session, org):
        """`destination_connection_id` is optional here — a transformation
        without one inherits its pipeline's destination. The new check must not
        turn None into a refusal."""
        t = TransformationService().create_transformation(
            db_session, org.id, "t_none", "select 1", Materialization.VIEW
        )
        assert t.destination_connection_id is None


class TestUploadsRefuseADestinationDltCannotLoadInto:
    """A DIFFERENT set — uploads never touch dbt.

    core#862 originally recorded *"uploads must keep offering MySQL"*, which was
    wrong: MySQL is not a load destination either, and never was. But the sets
    are still genuinely different, and these tests are what stop them being
    collapsed — Databricks is refused for pipelines and accepted here.
    """

    def test_mysql_is_refused(self, conn_svc, db_session, org):
        src = _conn(conn_svc, db_session, org, ConnectionType.POSTGRES)
        dst = _conn(conn_svc, db_session, org, ConnectionType.MYSQL)
        with pytest.raises(ValueError, match="cannot load into"):
            UploadService(conn_svc).create_upload(
                db_session, org.id, "u", None, src.id, dst.id, {"mode": "full_database"}
            )

    def test_sqlite_is_refused(self, conn_svc, db_session, org):
        src = _conn(conn_svc, db_session, org, ConnectionType.POSTGRES)
        dst = _conn(conn_svc, db_session, org, ConnectionType.SQLITE)
        with pytest.raises(ValueError, match="cannot load into"):
            UploadService(conn_svc).create_upload(
                db_session, org.id, "u", None, src.id, dst.id, {"mode": "full_database"}
            )

    def test_databricks_is_accepted_here(self, conn_svc, db_session, org):
        """🔑 The test that stops the two sets being merged.

        Databricks is refused for pipelines (no dbt adapter) and accepted for
        uploads (dlt loads into Delta tables). Any implementation that uses one
        set for both fails exactly here, and nowhere else.
        """
        src = _conn(conn_svc, db_session, org, ConnectionType.POSTGRES)
        dst = _conn(conn_svc, db_session, org, ConnectionType.DATABRICKS)
        upload = UploadService(conn_svc).create_upload(
            db_session, org.id, "uok", None, src.id, dst.id, {"mode": "full_database"}
        )
        assert upload.destination_connection_id == dst.id

    def test_mysql_is_still_a_valid_source(self, conn_svc, db_session, org):
        """The over-correction control.

        Extraction from MySQL works and is verified against a real MySQL server
        in `test_mysql_after_dbt_mysql_removal.py`. Withdrawing the destination
        role must not withdraw this one — and an assertion that MySQL is
        "unsupported" would pass against a product that had deleted it.
        """
        src = _conn(conn_svc, db_session, org, ConnectionType.MYSQL)
        dst = _conn(conn_svc, db_session, org, ConnectionType.POSTGRES)
        upload = UploadService(conn_svc).create_upload(
            db_session, org.id, "umysqlsrc", None, src.id, dst.id, {"mode": "full_database"}
        )
        assert upload.source_connection_id == src.id
