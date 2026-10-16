<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT-0
-->

# Contributing

Contributions are welcome. Changes must follow `AGENTS.md` and pass:

Optionally install the local pre-commit hook (fmt check + strict Clippy) with:

```text
git config core.hooksPath hooks
```

Hooks can be bypassed with `git commit --no-verify`; the CI run remains the authoritative gate.

```text
cargo fmt --all -- --check
bash scripts/ci/check_spdx_headers.sh
bash scripts/ci/check_websocket_features.sh
bash scripts/ci/check_proxy_isolation.sh
cargo clippy --all-targets --all-features --locked -- -D warnings -D clippy::pedantic -D clippy::await_holding_lock -D clippy::expect_used -D clippy::unwrap_used
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps --locked
cargo nextest run --all-features --locked --no-fail-fast
cargo test --doc --all-features --locked
cargo package --locked
cargo deny --locked check
cargo audit --file Cargo.lock --deny warnings
gitleaks git --no-banner --redact --log-opts="--all" .
```

Never add live credentials, captured account data, private-platform dependencies, or generated
provider tokens. Tests must be deterministic and use synthetic fixtures unless they are explicitly
ignored read-only live probes.

## Local build cache

Kache is optional and speeds up only local builds of this repository. Crates that
depend on this one never use it, and CI does not use it (it caches with
`Swatinem/rust-cache`). To opt in, install it and run its one-time setup, which
covers every repository on the machine:

```sh
cargo install kache --locked
kache init
kache doctor
```

[.kache.toml](.kache.toml) keeps the cache local-only, with no remote backends or
planner, and isolates this repository's entries under its own key salt;
`kache doctor` lists it as the config file. The
[Kache quick start](https://ninja.kunobi.com/docs/kache/getting-started/quick-start)
covers the rest.
