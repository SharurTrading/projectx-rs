<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT
-->

# Contributing

Contributions are welcome. Changes must follow `AGENTS.md` and pass:

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
