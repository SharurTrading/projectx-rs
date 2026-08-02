<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT
-->

# Changelog

All notable changes will be documented here. This project follows Semantic Versioning after its
first public release.

## Unreleased

- License the project under the MIT License and add SPDX file headers.
- Establish the standalone ProjectX REST client boundary.
- Add exact decimal models, typed identifiers, redacted credentials, and deterministic fixtures.
- Cover every documented REST endpoint, including contract-by-ID and partial position close.
- Add bounded SignalR market/user hubs with validated handshakes, invocation completion,
  reconnect notification, rotating-token snapshots, and explicit gap recovery.
- Preserve unknown provider enum codes and add an ignored read-only live probe.
- Enforce ProjectX rolling-window REST limits across cloned clients, honor `Retry-After`, and reject
  locally throttled mutations before network submission.
