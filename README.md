# POST PULSAR

POST PULSAR is a Python 3.12 social publishing application for explicit
one-shot operation and durable foreground-daemon scheduling. Anson Phong is the
original author and copyright holder.

## Minimum installation

Copy `post-pulsar.toml.example` to `post-pulsar.toml`, review its non-secret
settings, and provision the referenced environment variables outside version
control. The setup scripts create a project-local `.venv`, bootstrap the pinned
`uv`, and install from `uv.lock` with `uv sync --frozen`. They never upgrade the
operating system or install background startup automatically.

On Linux or macOS:

```sh
./setup-post-pulsar.sh
```

On Windows:

```batch
setup-post-pulsar.bat
```

If `.venv` was created by the other operating-system family, setup stops with a
manual review/removal instruction. It never deletes an existing environment.

## Minimum run commands

The launchers resolve the repository directory, use its `.venv`, forward
one-shot CLI arguments, and return the application's exit code:

```sh
./run-post-pulsar.sh --help
./run-post-pulsar.sh profiles list
./run-post-pulsar.sh daemon
```

The Windows equivalents use `run-post-pulsar.bat`. The `daemon` launcher form
maps to `python -m post_pulsar daemon foreground`; it is the sole built-in
scheduling authority.

User background startup is always an explicit convenience action and is not an
agent sandbox. On Linux/macOS, inspect `service/` and use
`setup-post-pulsar.sh --install-user-service`. On Windows, inspect and run
`task-setup-post-pulsar.bat install`. These integrations start the foreground
daemon rather than scheduling individual publications.

For a recommended separate-identity deployment, first run
`setup-post-pulsar.sh --hardened-guide` or
`setup-post-pulsar.bat --hardened-guide`. The guide reports the required
daemon-owned state/publishable boundary and agent-only DRAFTS access; it does
not create identities or change permissions. Hardened deployments use an
operator-enabled system service and disable MCP auto-start.

## License

GPL-3.0. See `LICENSE.md`.

Copyright (C) 2024 Anson Phong
