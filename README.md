# migration-suite

`migration-suite` is a Ratatui application for packaging Git bundles, Helm charts, and Docker images for transfer into an airgapped environment.

The app is CLI-backed in v1. It expects working `git`, `helm`, and `docker` installations with any required authentication already configured on the operator machine.

## Features

- Tabbed TUI for `Git`, `Helm`, `Docker`, `Jobs`, and `Config`
- Typed split configuration with light in-app editing
- Preview before export for Git, Helm, and Docker jobs
- Timestamped run directories with manifests, logs, and SHA-256 checksums
- Git export detection based on commits or tags in a preset time window
- Git repos are refreshed from their configured remote before preview/export
- Combined Git and Helm payloads, plus one Docker payload per selected image

## Configuration

Create `migration-suite.toml` in the project root:

```toml
[output]
base_dir = "migration-exports"
recent_run_limit = 10
split_large_transfers = false
max_transfer_size_mb = 200

[includes]
git = "migration-suite.git.toml"
helm = "migration-suite.helm.toml"
docker = "migration-suite.docker.toml"
```

Create `migration-suite.git.toml`:

```toml
[git]
default_branches = ["develop", "release/abc", "release/xyz"]

[[git.repos]]
name = "auth-service"
path = "/Users/jack/work/auth-service"
remote = "origin"
enabled = true

[[git.repos]]
name = "user-api"
path = "/Users/jack/work/user-api"
remote = "origin"
branches = ["develop", "release/abc"]
enabled = true
```

Create `migration-suite.helm.toml`:

```toml
[helm]

[[helm.charts]]
name = "backend"
reference = "oci://harbor.example.local/charts/backend"
version = "1.2.3"
enabled = true
```

Create `migration-suite.docker.toml`:

```toml
[docker]

[[docker.images]]
name = "user-api"
repository = "harbor.example.local/apps/user-api"
tag = "0.3.4-dev"
enabled = true
```

## Running

```bash
cargo run
```

## Controls

- `Tab` / `Shift+Tab`: switch tabs
- `q`: quit
- `Up` / `Down`: move selection
- `Space`: toggle the current repo/chart/image or config item
- `p`: build a preview on the active packaging tab
- `Enter`: start the previewed job from the modal
- `e`: edit the selected Helm chart or Docker image from its tab

Config tab:

- `Left` / `Right`: switch config sections
- `e`: edit the selected section or item
- `a`: add a repo/chart/image
- `d`: delete the selected repo/chart/image
- `s`: save `migration-suite.toml`

Git tab:

- `Left` / `Right`: switch preset time windows

Jobs tab:

- `r`: reload manifests from disk

## Output Layout

Each run creates a timestamped directory under `output.base_dir`:

```text
migration-exports/
  git-2026-03-14_22-00-00/
    git/
      auth-service/
        bundle
      user-api/
        bundle
    Git-migration_2026-03-14_22-00-00.tar.gz
    Git-migration_2026-03-14_22-00-00.tar.gz.txt
    job.log
    manifest.json
```

Helm runs create a combined `helm-charts_<timestamp>.tar.gz.txt` payload. Docker runs create one payload per selected image, for example `user-api_0.3.4-dev.tar.gz.txt`.

If `output.split_large_transfers` is enabled and a final transfer payload exceeds `output.max_transfer_size_mb`, the app splits it into numbered files such as `Git-migration_<timestamp>.tar.gz.part001.txt`, `part002.txt`, and so on.

## Notes

- `git.repos[].remote` controls which remote is fetched before Git preview/export. If omitted, the app uses `origin`.
- `output.split_large_transfers` controls whether oversized transfer payloads are automatically split into numbered `.partNN.txt` files.
- New installs default to split config files when you save from the TUI. Existing single-file configs still load and save correctly.
- Git LFS export is not implemented in v1 and is called out in the Git manifest notes.
- Docker exports run sequentially by default.
- Recent run history is loaded from manifest files on disk; there is no separate job database in v1.

## License

Copyright (c) Jack <60209373+OpoJack@users.noreply.github.com>

This project is licensed under the MIT license ([LICENSE] or <http://opensource.org/licenses/MIT>)

[LICENSE]: ./LICENSE
