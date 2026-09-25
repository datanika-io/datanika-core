"""Asana's `tasks` resource must carry a scope Asana accepts (core#1574).

`GET /tasks` is invalid without one, so `{"name": "tasks", "endpoint": {"path": "tasks"}}` — what
shipped — could never load, on a connector whose docs say *"sync Asana projects and **tasks** into
your warehouse"*. Asana is a task tracker; `tasks` is the table the connector exists for.

Measured against the live API, 2026-09-25, with `secrets/asana.env`:

    GET /tasks                                   -> 400  "You should specify one of workspace,
                                                          project, tag, section, user_task_list"
    GET /tasks?workspace=<gid>                   -> 400  "Must specify exactly one of project, tag,
                                                          section, user task list, or assignee +
                                                          workspace"
    GET /tasks?workspace=<gid>&assignee=me       -> 200
    GET /tasks?project=<a gid that is not mine>  -> 403  "You do not have access to this project."
    GET /tasks?project=  (empty string)          -> 400  "project: Not a Long"
    controls: no auth header -> 401 · garbage bearer -> 401

🔑 **The second line is the finding the issue did not have.** Its AC2 asked for *"a workspace (and
ideally project) field"*, and a `workspace` field **alone does not fix this** — it trades one 400
for another. The issue's own passing measurement was `workspace=<gid>&assignee=me`, and nothing
recorded that the `assignee` half was load-bearing.

So the fix scopes `tasks` **per project**, which is also what `SPEC_WAVE1_CONNECTOR_FIELDS` §4 said
in July: *"there is no 'all tasks in a workspace' endpoint — tasks must be iterated per project (or
per assignee+workspace). The extractor must fetch `projects` first, then loop them."* The spec named
the mechanism and the shipped code took the minimal path.

⚠️ `workspace + assignee` was rejected as the default deliberately: it loads only the connecting
user's tasks, which is a silently partial table, and a partial table is the failure mode this whole
issue is an instance of.

## The dlt shape, measured against dlt and not against its documentation

The parent reference must be a **braced format placeholder**: `"{resources.projects.gid}"`.

* `{"type": "resolve", "resource": "projects", "field": "gid"}` — the dict form — is for **path**
  params. In `params` dlt raises ``ValueError: ... not bound in path `tasks` ``.
* `"resources.projects.gid"` **without braces** is the trap: dlt finds expressions with
  ``string.Formatter().parse``, so an unbraced value carries no field name. The source **builds**,
  all five resources are present, and `tasks.is_transformer` is `False` with no parent — dlt then
  sends the literal text `resources.projects.gid` as `project`, which Asana answers `400 project:
  Not a Long`. **A silently wrong build whose only tell is the dependency graph.**

That is why :class:`TestTheParentReferenceActuallyBinds` asserts the graph rather than the string.
"""

from __future__ import annotations

import pytest

from datanika.services.connection_schemas import CONFIG_SCHEMAS
from datanika.services.dlt_runner import DltRunnerService, asana_default_resources

#: The scopes Asana's own 400 names, i.e. the ones that satisfy its validator.
ACCEPTED_SCOPES = frozenset({"project", "tag", "section", "user_task_list", "assignee"})


def _by_name(resources) -> dict:
    return {r["name"]: r for r in resources}


def _params(resource) -> dict:
    return dict((resource.get("endpoint") or {}).get("params") or {})


class TestTheDefaultResourceListScopesTasks:
    def test_tasks_carries_a_scope(self):
        tasks = _by_name(asana_default_resources())["tasks"]
        assert set(_params(tasks)) & ACCEPTED_SCOPES, (
            "GET /tasks with no scope is a 400 against the live API: " + repr(tasks)
        )

    def test_the_scope_is_a_project_reference_and_not_an_empty_value(self):
        """An empty `project` is its own 400 (`project: Not a Long`), so a scope key present with a
        blank value is not a fix. The value must be the parent reference."""
        tasks = _by_name(asana_default_resources())["tasks"]
        assert _params(tasks).get("project") == "{resources.projects.gid}", _params(tasks)

    def test_projects_is_in_the_list_so_the_reference_resolves(self):
        names = set(_by_name(asana_default_resources()))
        assert "projects" in names, (
            "`tasks` resolves its project from the `projects` resource, so removing `projects` "
            "from the default list makes the source fail to build"
        )

    def test_the_five_advertised_resources_are_all_still_there(self):
        """`/docs/connectors/asana` Step 3 lists exactly these, ticked by default."""
        assert set(_by_name(asana_default_resources())) == {
            "workspaces",
            "projects",
            "tasks",
            "users",
            "tags",
        }

    def test_the_offered_endpoint_names_still_match_the_picker(self):
        from datanika.ui.state.connection_state import SAAS_DEFAULT_ENDPOINTS

        assert sorted(_by_name(asana_default_resources())) == sorted(
            SAAS_DEFAULT_ENDPOINTS["asana"]
        ), (
            "the endpoint picker offers names the builder does not produce, which is core#532: "
            "unticking a resource that does not exist changes nothing and looks like it worked"
        )


class TestTheWorkspaceFieldScopesProjectsAndNotTasks:
    """AC2 + AC3. The field narrows which projects load; it is never put on `/tasks`.

    ⚠️ Putting it on `/tasks` is the obvious reading of the issue and is measured wrong (400).
    """

    def test_a_workspace_scopes_the_projects_resource(self):
        projects = _by_name(asana_default_resources("12345"))["projects"]
        assert _params(projects) == {"workspace": "12345"}

    def test_no_workspace_leaves_projects_unscoped_rather_than_sending_a_blank(self):
        """`GET /projects` with no workspace is a measured 200, so the honest default is to omit
        the param — not to send `workspace=`."""
        projects = _by_name(asana_default_resources())["projects"]
        assert "workspace" not in _params(projects), _params(projects)

    @pytest.mark.parametrize("blank", ["", "   ", None])
    def test_a_blank_workspace_is_the_same_as_none(self, blank):
        projects = _by_name(asana_default_resources(blank))["projects"]
        assert "workspace" not in _params(projects), (
            f"a blank workspace ({blank!r}) reached the request as a parameter; Asana answers a "
            "blank scope with its own 400 rather than ignoring it"
        )

    def test_the_workspace_is_never_added_to_tasks(self):
        tasks = _by_name(asana_default_resources("12345"))["tasks"]
        assert "workspace" not in _params(tasks), (
            "`workspace` on /tasks is a 400 unless `assignee` accompanies it (measured "
            "2026-09-25). It scopes /projects, and /tasks inherits the narrowing through the "
            "project reference."
        )


class TestTheParentReferenceActuallyBinds:
    """The assertion the string form cannot make: dlt must build a real dependency.

    An unbraced reference builds a source with all five resources and no dependency at all, so
    every name-based assertion above passes while the request carries a literal string. Only the
    dependency graph separates the two.

    ⚠️ Scoped opt-in, not module-wide: this is the only class here that BUILDS. The others read
    the static resource list and the schema, so a module-wide fixture would protect nothing
    (a fixture with no hot path reads exactly like a working one).
    """

    @pytest.fixture(autouse=True)
    def _stub_dns(self, no_live_dns):
        """Building a source resolves `app.asana.com`, so this class did live DNS (core#1597).

        🔑 **This class is the SIXTH module core#1597 turned up, and it was caught by CI rather
        than by my own sweep — because the sweep's module pattern grepped for `build_source`,
        which is NOT a substring of `_build_saas_source`.** The entry point here is the private
        one, so the module never entered the population and was never run with the resolver
        refused. `app.asana.com` resolves on a dev machine and in CI, so it passed both.

        That is the same instrument error core#1597 is *about*, committed while fixing it: the
        population was derived from a pattern nobody had seen fail. The directory-wide refusal in
        `conftest.py` is what made the gap visible at all — a census keyed on the public entry
        point would have missed it again.
        """
        return no_live_dns

    @staticmethod
    def _source(config):
        return DltRunnerService()._build_saas_source("asana", config, {})

    def test_tasks_is_a_transformer_whose_parent_is_projects(self):
        src = self._source({"api_key": "t"})
        tasks = src.resources["tasks"]
        assert tasks.is_transformer, (
            "`tasks` is not a transformer, so dlt created no dependency and the project reference "
            "is being sent to Asana as a literal string — check the braces"
        )
        parent = getattr(getattr(tasks, "_pipe", None), "parent", None)
        assert getattr(parent, "name", None) == "projects", parent

    def test_the_other_resources_are_not_transformers(self):
        """The negative half: if everything were a transformer the assertion above would be
        satisfied by a builder that made every resource dependent on something."""
        src = self._source({"api_key": "t"})
        plain = ("workspaces", "projects", "users", "tags")
        assert not [n for n in plain if src.resources[n].is_transformer]

    def test_selecting_only_tasks_keeps_its_parent_in_the_source(self):
        """The endpoint picker calls `with_resources`, so a user who ticks only `tasks` must still
        get `projects` fetched to feed it — otherwise narrowing the load breaks it."""
        narrowed = self._source({"api_key": "t"}).with_resources("tasks")
        assert sorted(narrowed.selected_resources) == ["tasks"]
        assert "projects" in narrowed.resources

    def test_an_unbraced_reference_would_not_bind(self):
        """The control, driven with the wrong shape. This is what makes the assertions above
        readings rather than restatements: dlt accepts the unbraced form silently."""
        from dlt.sources.rest_api import rest_api_source

        src = rest_api_source(
            {
                "client": {"base_url": "https://app.asana.com/api/1.0/"},
                "resources": [
                    {"name": "projects", "endpoint": {"path": "projects"}},
                    {
                        "name": "tasks",
                        # deliberately unbraced
                        "endpoint": {
                            "path": "tasks",
                            "params": {"project": "resources.projects.gid"},
                        },
                    },
                ],
            }
        )
        assert not src.resources["tasks"].is_transformer, (
            "dlt now binds an unbraced parent reference, so the braces are no longer the "
            "discriminator this module relies on — re-read the fix against dlt's current behaviour"
        )


class TestTheScopingFieldReachesTheUser:
    """AC2 — the schema, the form and the runner are three hand-maintained vocabularies (core#662).

    The round-trip ratchet in ``tests/test_ui/test_connection_config_roundtrip.py`` covers the
    form's two serialisers. What it cannot see is whether the runner reads the same key, which is
    the third vocabulary and the one `slack` and `salesforce` still get wrong.
    """

    def test_the_schema_declares_workspace_and_leaves_it_optional(self):
        schema = CONFIG_SCHEMAS["asana"]
        assert "workspace" in schema["properties"], (
            "no scoping field reaches the user, so `tasks` can only ever load whatever the "
            "default enumeration finds"
        )
        assert "workspace" not in schema["required"], (
            "workspace is optional: `GET /projects` with no workspace is a measured 200, and the "
            "form must not refuse a blank for a value it does not need"
        )
        assert schema["required"] == ["api_key"]

    def test_the_runner_reads_the_key_the_schema_declares(self):
        """Not a restatement: it asserts the value travels from a config dict into the request."""
        gid = "999888777"
        projects = _by_name(
            DltRunnerService._asana_resources_for({"api_key": "t", "workspace": gid})
        )["projects"]
        assert _params(projects) == {"workspace": gid}

    def test_the_form_serialisers_both_carry_it(self):
        """The core#638 pairing: a key in one serialiser and not the other is dropped on save."""
        import inspect

        from datanika.ui.state.connection_state import ConnectionState

        build = inspect.getsource(ConnectionState._build_config)
        populate = inspect.getsource(ConnectionState._populate_form_from_config)
        assert "form_workspace" in build, "`workspace` is not written by _build_config"
        assert "form_workspace" in populate, "`workspace` is not read by _populate_form_from_config"
