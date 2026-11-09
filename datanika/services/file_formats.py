"""Which file extensions each file connector supports — the ONE declaration (core#1604).

Three places used to carry their own copy of this list, and on 2026-09-26 all three
disagreed with each other and with our own connector guides:

* the New Connection file picker's ``accept`` filter — ``.csv``, ``.json``, ``.parquet``;
* :data:`datanika.services.file_upload_service.ALLOWED_EXTENSIONS`, the copy that
  **refuses** an upload — ``csv``, ``json``, ``parquet``;
* ``dlt_runner.FILE_FORMAT_BY_EXTENSION``, which picks a reader for a glob — the widest of
  the three, and the only one that already knew ``.jsonl`` and ``.ndjson``.

So a reader who followed the ``json`` guide to *"drag your `.json` / `.jsonl` / `.ndjson`
file"* was shown a browse dialog that did not list their file: no error, nothing to search
for. Five documented extensions were affected, not the three [core#1604] named — the issue
was filed from a walk of the ``json`` and ``parquet`` guides, and ``csv.md`` documents
``.tsv`` and ``.txt`` in the same sentence shape.

🔑 **Derive, never re-list.** The three copies drifted because nothing connected them, and
the drift was invisible: each list is locally plausible and none of them is wrong on its
own. ``tests/test_services/test_file_formats.py`` asserts that every consumer *references*
this module rather than restating its contents, and compares this declaration against a
dated, cited transcription of the guides in both directions.

⚠️ **Adding an extension here widens what a user may upload.** That is the point, but it is
also the reason the guard asks for a guide citation: an extension in the picker that no
guide names is a promise nothing backs, and the too-wide direction fails silently while the
too-narrow one at least produces a confused user.
"""

#: Extensions each file connector type supports. **The single source of truth.**
#:
#: ``.tsv`` and ``.txt`` read through pandas' CSV reader — the delimiter is the user's to set
#: on the upload, exactly as it is for a semicolon-separated ``.csv``.
#:
#: ``.parq`` and ``.pq`` are both real Parquet extensions in the wild (dask and fastparquet
#: emit them). ``.pq`` was already resolvable before core#1604 and is named in no guide;
#: ``.parq`` is named in ``parquet.md`` and was resolvable nowhere.
FILE_EXTENSIONS_BY_TYPE: dict[str, tuple[str, ...]] = {
    "csv": (".csv", ".tsv", ".txt"),
    "json": (".json", ".jsonl", ".ndjson"),
    "parquet": (".parquet", ".parq", ".pq"),
}

#: MIME type to file the extensions under in the browse dialog's filter.
#:
#: react-dropzone's ``accept`` is keyed by MIME type with extensions as values, and
#: ``attr-accept`` admits a file when **either** matches — so an extension listed here is
#: selectable regardless of the MIME type the browser reports for it. That matters for
#: ``.tsv`` (often ``text/tab-separated-values``) and ``.txt`` (``text/plain``), neither of
#: which is ``text/csv``. Parquet has no registered type, hence the octet-stream bucket;
#: these three keys are unchanged from before core#1604.
FILE_MIME_TYPE_BY_TYPE: dict[str, str] = {
    "csv": "text/csv",
    "json": "application/json",
    "parquet": "application/octet-stream",
}

#: Extension → the reader that can read it. Consumed by ``dlt_runner._resolve_file_format``
#: for glob-driven sources (``s3``), which carry no format in their connection type.
#:
#: Derived, so a new extension above reaches the reader without a second edit. Flat, so an
#: extension claimed by two types would resolve to whichever came last —
#: ``test_each_declared_extension_belongs_to_exactly_one_type`` refuses that.
FILE_FORMAT_BY_EXTENSION: dict[str, str] = {
    ext: file_type for file_type, exts in FILE_EXTENSIONS_BY_TYPE.items() for ext in exts
}

#: The upload allowlist, without the leading dot — ``FileUploadService`` compares against the
#: tail of ``filename.rsplit(".", 1)``. This is the copy that *refuses*, so it is the one
#: whose narrowness a user meets as an error message.
UPLOAD_ALLOWED_EXTENSIONS: frozenset[str] = frozenset(
    ext.lstrip(".") for ext in FILE_FORMAT_BY_EXTENSION
)


def upload_accept_map() -> dict[str, list[str]]:
    """Build the file picker's ``accept`` filter from :data:`FILE_EXTENSIONS_BY_TYPE`.

    A fresh dict each call: Reflex stores the value into a generated ``useDropzone``
    argument and a shared mutable default is one refactor away from being edited in place.

    Returns:
        MIME type → the extensions to offer under it, for ``rx.upload(accept=...)``.
    """
    return {
        FILE_MIME_TYPE_BY_TYPE[file_type]: list(exts)
        for file_type, exts in FILE_EXTENSIONS_BY_TYPE.items()
    }
