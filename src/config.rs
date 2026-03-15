use std::{
    fs,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, eyre};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppConfig {
    #[serde(default)]
    pub output: OutputConfig,
    #[serde(default)]
    pub git: GitConfig,
    #[serde(default)]
    pub helm: HelmConfig,
    #[serde(default)]
    pub docker: DockerConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            output: OutputConfig::default(),
            git: GitConfig::default(),
            helm: HelmConfig::default(),
            docker: DockerConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)?;
        let config = toml::from_str::<Self>(&raw)?;
        config.validate()?;
        Ok(config)
    }

    pub fn load_or_default(path: &Path) -> Result<Self> {
        if path.exists() {
            Self::load(path)
        } else {
            Ok(Self::default())
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let raw = toml::to_string_pretty(self)?;
        fs::write(path, raw)?;
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.output.base_dir.as_os_str().is_empty() {
            return Err(eyre!("output.base_dir cannot be empty"));
        }
        if self.output.recent_run_limit == 0 {
            return Err(eyre!("output.recent_run_limit must be greater than zero"));
        }
        if self.git.default_branches.is_empty() {
            return Err(eyre!(
                "git.default_branches must include at least one branch"
            ));
        }
        for repo in &self.git.repos {
            if repo.name.trim().is_empty() {
                return Err(eyre!("git repo names cannot be empty"));
            }
            if repo.path.as_os_str().is_empty() {
                return Err(eyre!("git repo path for {} cannot be empty", repo.name));
            }
        }
        for chart in &self.helm.charts {
            if chart.name.trim().is_empty()
                || chart.reference.trim().is_empty()
                || chart.version.trim().is_empty()
            {
                return Err(eyre!(
                    "helm chart entries require name, reference, and version"
                ));
            }
        }
        for image in &self.docker.images {
            if image.name.trim().is_empty()
                || image.repository.trim().is_empty()
                || image.tag.trim().is_empty()
            {
                return Err(eyre!(
                    "docker image entries require name, repository, and tag"
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputConfig {
    #[serde(default = "default_output_dir")]
    pub base_dir: PathBuf,
    #[serde(default = "default_recent_run_limit")]
    pub recent_run_limit: usize,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            base_dir: default_output_dir(),
            recent_run_limit: default_recent_run_limit(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitConfig {
    #[serde(default = "default_git_branches")]
    pub default_branches: Vec<String>,
    #[serde(default)]
    pub repos: Vec<GitRepoConfig>,
}

impl Default for GitConfig {
    fn default() -> Self {
        Self {
            default_branches: default_git_branches(),
            repos: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitRepoConfig {
    pub name: String,
    pub path: PathBuf,
    #[serde(default)]
    pub remote: Option<String>,
    #[serde(default)]
    pub branches: Option<Vec<String>>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

impl GitRepoConfig {
    pub fn branches<'a>(&'a self, defaults: &'a [String]) -> &'a [String] {
        self.branches.as_deref().unwrap_or(defaults)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct HelmConfig {
    #[serde(default)]
    pub charts: Vec<HelmChartConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HelmChartConfig {
    pub name: String,
    pub reference: String,
    pub version: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DockerConfig {
    #[serde(default)]
    pub images: Vec<DockerImageConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DockerImageConfig {
    pub name: String,
    pub repository: String,
    pub tag: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

pub fn config_path_from_cwd(cwd: &Path) -> PathBuf {
    cwd.join("migration-suite.toml")
}

fn default_output_dir() -> PathBuf {
    PathBuf::from("migration-exports")
}

fn default_recent_run_limit() -> usize {
    10
}

fn default_git_branches() -> Vec<String> {
    vec!["develop".to_string()]
}

fn default_enabled() -> bool {
    true
}

pub fn branches_to_csv(branches: &[String]) -> String {
    branches.join(",")
}

pub fn csv_to_branches(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn parses_and_roundtrips_toml() {
        let raw = r#"
[output]
base_dir = "exports"
recent_run_limit = 5

[git]
default_branches = ["develop", "release/abc"]

[[git.repos]]
name = "user-api"
path = "/tmp/user-api"
remote = "origin"
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
enabled = false
"#;

        let config: AppConfig = toml::from_str(raw).expect("config should parse");
        config.validate().expect("config should validate");

        let encoded = toml::to_string_pretty(&config).expect("config should encode");
        let decoded: AppConfig = toml::from_str(&encoded).expect("config should roundtrip");

        assert_eq!(config, decoded);
    }

    #[test]
    fn saves_and_loads_config() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("migration-suite.toml");
        let config = AppConfig::default();

        config.save(&path).expect("save");
        let loaded = AppConfig::load(&path).expect("load");

        assert_eq!(config, loaded);
    }

    #[test]
    fn branches_csv_helpers_trim_and_filter() {
        let branches = csv_to_branches("develop, release/abc ,, release/xyz");
        assert_eq!(branches, vec!["develop", "release/abc", "release/xyz"]);
        assert_eq!(
            branches_to_csv(&branches),
            "develop,release/abc,release/xyz"
        );
    }
}
