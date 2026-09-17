# Unreleased

Notes for the next `v0.x` tag. Self-hoster-facing: written for someone who runs
`git checkout <tag> && docker compose up -d --build` and needs to know what will behave
differently afterwards.

> ⚠️ **This file does not reach the GitHub Release yet.** `release.yml` runs
> `gh release create --generate-notes` with no notes file, so the published notes are the
> auto-generated PR-title list and nothing else ([core#680]). Until #680 ships the optional
> `--notes-file` step, this file is reviewable in the PR that cuts the release but is **not**
> what a self-hoster sees on the release page. Whoever implements #680 should wire it to
> `docs/release-notes/<tag>.md`; this is the first file written for that convention.

## Breaking

None recorded since `v0.2.1`. Add a section here for anything that behaves differently for a
self-hoster after the next tag, in the shape `v0.2.0.md` uses: who this affects, what changed, and what
to do. The file is rotated to `<tag>.md` when that tag is cut.
