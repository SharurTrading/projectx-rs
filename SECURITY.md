<!--
SPDX-FileCopyrightText: 2026 Kevin Monaghan
SPDX-License-Identifier: MIT
-->

# Security

Please report suspected vulnerabilities privately to the repository owner. Do not open a public
issue containing credentials, bearer tokens, account identifiers, order identifiers, or captured
provider payloads.

The library does not read `.env` files, persist secrets, or expose bearer tokens. Callers own secret
acquisition and storage. Logs and bug reports must use synthetic or redacted data.
