# migration-suite

`migration-suite` is a Ratatui application for packaging Git bundles, Helm charts, and Docker images for transfer into an airgapped environment.

The app is CLI-backed in v1. It expects working `git`, `helm`, and `docker` installations with any required authentication already configured on the operator machine.

## Features

- Tabbed TUI for `Git`, `Helm`, `Docker`, `Jobs`, and `Config`
- Typed `migration-suite.toml` configuration with light in-app editing
- Preview before export for Git, Helm, and Docker jobs
- Timestamped run directories with manifests, logs, and SHA-256 checksums
- Git export detection based on commits or tags in a preset time window
- Combined Git and Helm payloads, plus one Docker payload per selected image

## Configuration

Create `migration-suite.toml` in the project root:

```toml
[output]
base_dir = "migration-exports"
recent_run_limit = 10

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
branches = ["develop", "release/abc"]
enabled = true

[[helm.charts]]
name = "backend"
reference = "oci://harbor.example.local/charts/backend"
version = "1.2.3"
enabled = true

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
  git-20260314T220000Z/
    git/
      auth-service/
        bundle
      user-api/
        bundle
    Git-migration_20260314T220000Z.tar.gz
    Git-migration_20260314T220000Z.tar.gz.txt
    job.log
    manifest.json
```

Helm runs create a combined `helm-charts_<timestamp>.tar.gz.txt` payload. Docker runs create one payload per selected image, for example `user-api_0.3.4-dev.tar.gz.txt`.

## Notes

- Git LFS export is not implemented in v1 and is called out in the Git manifest notes.
- Docker exports run sequentially by default.
- Recent run history is loaded from manifest files on disk; there is no separate job database in v1.

## License

Copyright (c) Jack <60209373+OpoJack@users.noreply.github.com>

This project is licensed under the MIT license ([LICENSE] or <http://opensource.org/licenses/MIT>)

[LICENSE]: ./LICENSE
