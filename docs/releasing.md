# Release process

1. Update `engine.__version__`, `pyproject.toml`, and `CHANGELOG.md` together.
2. Run the full PostgreSQL and chaos suite plus dependency and container scans.
3. Verify old open histories with `engine replay-check` against every retained
   pinned definition and document migration/rollback constraints.
4. Commit, push, and wait for CI and Security workflows to succeed.
5. Create a signed `vX.Y.Z` tag whose value exactly matches the package version.

Pushing the tag runs the release workflow. It rebuilds from `uv.lock`, repeats
the static/unit/security gate, creates wheel and source distributions, produces
a CycloneDX SBOM and SHA-256 manifest, generates GitHub provenance attestations,
and publishes the artifacts to a GitHub Release. No package registry publishing
occurs until repository ownership and publishing policy are explicitly chosen.

Verify a downloaded artifact with both `sha256sum --check SHA256SUMS` and
`gh attestation verify ARTIFACT --repo santinomarial/Durable-Workflow-Engine`.
