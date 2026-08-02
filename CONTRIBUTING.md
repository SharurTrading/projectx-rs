<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT
-->

# Contributing

Contributions are welcome. Changes must follow `AGENTS.md` and pass:

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
RUSTDOCFLAGS=-Dwarnings cargo doc --all-features --no-deps
```

Never add live credentials, captured account data, private-platform dependencies, or generated
provider tokens. Tests must be deterministic and use synthetic fixtures unless they are explicitly
ignored read-only live probes.
