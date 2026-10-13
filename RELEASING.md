<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT-0
-->

# Releasing projectx-client

Releases are cut from a pull request and published manually. The crate has no
credentialed release workflow; CI (`.github/workflows/ci.yml`) runs with
`contents: read` only, so tagging and publishing are operator steps performed
from a local checkout. This guide is the step-by-step form of the release gate
in `AGENTS.md`; if the two ever disagree, `AGENTS.md` wins.

The crate is the single package at the repository root
(`projectx-client` on crates.io, repository `SharurTrading/projectx-rs`).
There is no workspace, so one version in `Cargo.toml` is the release version.

## Choosing a version

Check both repository tags and crates.io before choosing the version — never
reuse or move a published one:

```bash
git fetch --tags
git tag -l 'v*'
cargo owner --list projectx-client   # also confirms publish ownership later
```

This project follows Semantic Versioning. Classify the public API diff since
the last release (start from the re-exports in `src/lib.rs`):

- **Major** — a breaking public API change: downstream source must change.
  Public enums are `#[non_exhaustive]` and IDs are validated newtypes, so
  adding an enum variant or a new public type is not by itself breaking.
- **Minor** — additive public API, new provider coverage, or a behaviour
  addition existing callers can ignore.
- **Patch** — fixes and dependency/CI-only updates with no API change.

A provider contract change can still force callers to update operational
behaviour (for example around ambiguous money-moving outcomes) even when the
SemVer bump is minor or patch; call that out in the changelog rather than
hiding it behind the number. The minimum supported Rust version is pinned in
both `rust-version` in `Cargo.toml` and `rust-toolchain.toml` (currently
1.95.0); raising it is at least a minor release and must be recorded in the
changelog.

Before the release PR, confirm the pre-merge items from `AGENTS.md`:

1. Provider terms permit the intended source distribution and branding.
2. Complete a secret and proprietary-content scan, including Git history
   (the `secret-scan` CI job runs Gitleaks over `--all`; see also
   `SECURITY.md` — no credentials, bearer tokens, captured account/order
   data, or provider payloads may ship).
3. All fixtures are synthetic and all documentation is public-safe.
4. The public API has been reviewed and the SemVer impact classified above.

## Release pull request

1. Start from an up-to-date `main` and create `release/X.Y.Z`.
2. Set `version` in `Cargo.toml` and refresh the crate's own entry in
   `Cargo.lock` (for example `cargo update --workspace`), then verify the
   lockfile diff is only the `projectx-client` version line.
3. This changelog keeps no standing `[Unreleased]` section: add
   `## [X.Y.Z] - YYYY-MM-DD` directly above the latest release in
   `CHANGELOG.md`, grouping entries under the headings already used there
   (`Added`, `Changed`, `Fixed`, and `Breaking changes` where applicable),
   and add the comparison link at the foot of the file:

   ```text
   [X.Y.Z]: https://github.com/SharurTrading/projectx-rs/compare/vA.B.C...vX.Y.Z
   ```

   Cite the issue or PR for each entry, as existing entries do.
4. Update `README.md` in the same PR (this was once a forgotten follow-up —
   see #83 — so do not split it out):
   - Every release: move both version-pinned badges — the crates.io badge
     (`crates/v/projectx-client.svg?v=X.Y.Z` linking to
     `crates.io/crates/projectx-client/X.Y.Z`) and the docs.rs badge
     (`docsrs/projectx-client/X.Y.Z?...` linking to
     `docs.rs/projectx-client/X.Y.Z/projectx_client/`).
   - Major releases only: update the installation major in
     `cargo add projectx-client@N` and `projectx-client = "N"`, and the
     "Version N follows Semantic Versioning" paragraph.
   - If the MSRV changed, update "The minimum supported Rust version is …".
5. Require a clean working tree before package/publish validation so the
   inspected archive exactly matches a PR commit.
6. Run the full gates (the same commands CI runs; `CONTRIBUTING.md` is the
   canonical list):

   ```bash
   cargo fmt --all -- --check
   bash scripts/ci/check_spdx_headers.sh
   bash scripts/ci/check_websocket_features.sh
   bash scripts/ci/check_proxy_isolation.sh
   cargo clippy --all-targets --all-features --locked -- \
     -D warnings -D clippy::pedantic -D clippy::await_holding_lock \
     -D clippy::expect_used -D clippy::unwrap_used
   RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps --locked
   cargo nextest run --all-features --locked --no-fail-fast
   cargo test --doc --all-features --locked
   cargo package --locked
   cargo deny --locked check
   cargo audit --file Cargo.lock --deny warnings
   gitleaks git --no-banner --redact --log-opts="--all" .
   cargo publish --dry-run --locked
   ```

   Release validation stays deterministic and credential-free
   (`PX-VALIDATE-01`). The ignored, read-only live probe in
   `tests/live_read_only.rs` is deliberately invoked by operators and is
   never a release gate; do not run it with real credentials to "check"
   a release.
7. Inspect `cargo package --list --locked`. The archive is limited by the
   `include` list in `Cargo.toml` (`src/`, `tests/`, `examples/`,
   `CHANGELOG.md`, `Cargo.lock`, `Cargo.toml`, `LICENSE`, `README.md`,
   `SECURITY.md`). Confirm no repository-only files slipped in (`AGENTS.md`,
   this guide, `.env`) and no expected file is missing.
8. Open the release pull request carrying the classification of the issue it
   closes (kind label and inherited Priority issue field, per
   `PROC-PR-TRIAGE` in `AGENTS.md`). Do not tag or publish from the branch.
   Any later commit requires the gates and archive checks to be rerun.

## Publish after merge

Publish only the exact merged commit on `main` from the reviewed release PR.
Direct pushes to protected branches, publishing from a dirty or unmerged
worktree, and moving or replacing a published version tag are prohibited.

1. Confirm the release pull request's exact merge commit passed the required
   `quality`, `supply-chain`, and `secret-scan` checks, and that the
   post-merge `main` CI run for that commit also passed.
2. Use a clean checkout of that merge commit and rerun the full gates above
   plus `cargo publish --dry-run --locked`.
3. Before creating a public tag, verify crates.io owner authentication with
   `cargo owner --list projectx-client` and GitHub authentication with
   `gh auth status`.
4. Create and push an annotated tag on that exact commit (tags are
   `v<version>`, matching `v6.1.1` and earlier):

   ```bash
   git tag -a vX.Y.Z -m "projectx-client X.Y.Z"
   git push origin vX.Y.Z
   ```

5. Publish the same clean tree with the crates.io owner credential:

   ```bash
   cargo publish --locked
   ```

6. Create the GitHub release from the verified tag and use the matching
   `CHANGELOG.md` section as its notes:

   ```bash
   gh release create vX.Y.Z --title "vX.Y.Z" --notes "<changelog section>"
   ```

7. Verify the release end to end:
   - the new version resolves on crates.io and a downstream project compiles
     against it (`cargo add projectx-client@X.Y.Z` in a scratch project, or
     the equivalent in the consumer being upgraded);
   - docs.rs built the new version successfully;
   - the README's version-pinned crates.io/docs.rs badges resolve to X.Y.Z.

Never move or reuse a published tag. If the crate is wrong after publication,
release a correction; yank only when the published version is unsafe to select.
