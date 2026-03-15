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
        Self::load_with_layout(path).map(|(config, _)| config)
    }

    pub fn load_or_default(path: &Path) -> Result<Self> {
        Self::load_or_default_with_layout(path).map(|(config, _)| config)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        self.save_with_layout(&ConfigLayout::inline(path))
    }

    pub fn load_with_layout(path: &Path) -> Result<(Self, ConfigLayout)> {
        let raw = fs::read_to_string(path)?;
        let root = toml::from_str::<RootConfigDocument>(&raw)?;

        let config = AppConfig {
            output: root.output,
            git: if let Some(include) = root.includes.git.as_ref() {
                load_git_config(resolve_include_path(path, include)?)?
            } else {
                root.git.unwrap_or_default()
            },
            helm: if let Some(include) = root.includes.helm.as_ref() {
                load_helm_config(resolve_include_path(path, include)?)?
            } else {
                root.helm.unwrap_or_default()
            },
            docker: if let Some(include) = root.includes.docker.as_ref() {
                load_docker_config(resolve_include_path(path, include)?)?
            } else {
                root.docker.unwrap_or_default()
            },
        };
        config.validate()?;

        Ok((
            config,
            if root.includes.is_split() {
                ConfigLayout {
                    root_path: path.to_path_buf(),
                    git_path: root
                        .includes
                        .git
                        .as_ref()
                        .map(|value| resolve_include_path(path, value))
                        .transpose()?,
                    helm_path: root
                        .includes
                        .helm
                        .as_ref()
                        .map(|value| resolve_include_path(path, value))
                        .transpose()?,
                    docker_path: root
                        .includes
                        .docker
                        .as_ref()
                        .map(|value| resolve_include_path(path, value))
                        .transpose()?,
                    use_split_files: true,
                }
            } else {
                ConfigLayout::inline(path)
            },
        ))
    }

    pub fn load_or_default_with_layout(path: &Path) -> Result<(Self, ConfigLayout)> {
        if path.exists() {
            Self::load_with_layout(path)
        } else {
            Ok((Self::default(), ConfigLayout::split_default(path)))
        }
    }

    pub fn save_with_layout(&self, layout: &ConfigLayout) -> Result<()> {
        self.validate()?;
        if layout.use_split_files {
            save_split_config(self, layout)
        } else {
            let raw = toml::to_string_pretty(self)?;
            fs::write(&layout.root_path, raw)?;
            Ok(())
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.output.base_dir.as_os_str().is_empty() {
            return Err(eyre!("output.base_dir cannot be empty"));
        }
        if self.output.recent_run_limit == 0 {
            return Err(eyre!("output.recent_run_limit must be greater than zero"));
        }
        if self.output.max_transfer_size_mb == 0 {
            return Err(eyre!(
                "output.max_transfer_size_mb must be greater than zero"
            ));
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigLayout {
    pub root_path: PathBuf,
    pub git_path: Option<PathBuf>,
    pub helm_path: Option<PathBuf>,
    pub docker_path: Option<PathBuf>,
    pub use_split_files: bool,
}

impl ConfigLayout {
    pub fn inline(root_path: &Path) -> Self {
        Self {
            root_path: root_path.to_path_buf(),
            git_path: None,
            helm_path: None,
            docker_path: None,
            use_split_files: false,
        }
    }

    pub fn split_default(root_path: &Path) -> Self {
        let parent = root_path.parent().unwrap_or_else(|| Path::new("."));
        Self {
            root_path: root_path.to_path_buf(),
            git_path: Some(parent.join("migration-suite.git.toml")),
            helm_path: Some(parent.join("migration-suite.helm.toml")),
            docker_path: Some(parent.join("migration-suite.docker.toml")),
            use_split_files: true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputConfig {
    #[serde(default = "default_output_dir")]
    pub base_dir: PathBuf,
    #[serde(default = "default_recent_run_limit")]
    pub recent_run_limit: usize,
    #[serde(default = "default_split_large_transfers")]
    pub split_large_transfers: bool,
    #[serde(default = "default_max_transfer_size_mb")]
    pub max_transfer_size_mb: u64,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            base_dir: default_output_dir(),
            recent_run_limit: default_recent_run_limit(),
            split_large_transfers: default_split_large_transfers(),
            max_transfer_size_mb: default_max_transfer_size_mb(),
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
struct RootConfigDocument {
    #[serde(default)]
    output: OutputConfig,
    #[serde(default)]
    includes: ConfigIncludes,
    #[serde(default)]
    git: Option<GitConfig>,
    #[serde(default)]
    helm: Option<HelmConfig>,
    #[serde(default)]
    docker: Option<DockerConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
struct ConfigIncludes {
    #[serde(default)]
    git: Option<PathBuf>,
    #[serde(default)]
    helm: Option<PathBuf>,
    #[serde(default)]
    docker: Option<PathBuf>,
}

impl ConfigIncludes {
    fn is_split(&self) -> bool {
        self.git.is_some() || self.helm.is_some() || self.docker.is_some()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
struct GitConfigDocument {
    #[serde(default)]
    git: GitConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
struct HelmConfigDocument {
    #[serde(default)]
    helm: HelmConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
struct DockerConfigDocument {
    #[serde(default)]
    docker: DockerConfig,
}

pub fn config_path_from_cwd(cwd: &Path) -> PathBuf {
    cwd.join("migration-suite.toml")
}

fn resolve_include_path(root_path: &Path, include: &Path) -> Result<PathBuf> {
    if include.is_absolute() {
        Ok(include.to_path_buf())
    } else {
        let base = root_path.parent().unwrap_or_else(|| Path::new("."));
        Ok(base.join(include))
    }
}

fn save_split_config(config: &AppConfig, layout: &ConfigLayout) -> Result<()> {
    let git_path = layout
        .git_path
        .clone()
        .ok_or_else(|| eyre!("split config layout is missing git_path"))?;
    let helm_path = layout
        .helm_path
        .clone()
        .ok_or_else(|| eyre!("split config layout is missing helm_path"))?;
    let docker_path = layout
        .docker_path
        .clone()
        .ok_or_else(|| eyre!("split config layout is missing docker_path"))?;

    let root = RootConfigDocument {
        output: config.output.clone(),
        includes: ConfigIncludes {
            git: Some(relative_include_path(&layout.root_path, &git_path)?),
            helm: Some(relative_include_path(&layout.root_path, &helm_path)?),
            docker: Some(relative_include_path(&layout.root_path, &docker_path)?),
        },
        git: None,
        helm: None,
        docker: None,
    };

    fs::write(&layout.root_path, toml::to_string_pretty(&root)?)?;
    fs::write(
        git_path,
        toml::to_string_pretty(&GitConfigDocument {
            git: config.git.clone(),
        })?,
    )?;
    fs::write(
        helm_path,
        toml::to_string_pretty(&HelmConfigDocument {
            helm: config.helm.clone(),
        })?,
    )?;
    fs::write(
        docker_path,
        toml::to_string_pretty(&DockerConfigDocument {
            docker: config.docker.clone(),
        })?,
    )?;
    Ok(())
}

fn relative_include_path(root_path: &Path, included_path: &Path) -> Result<PathBuf> {
    let root_parent = root_path.parent().unwrap_or_else(|| Path::new("."));
    if let Ok(relative) = included_path.strip_prefix(root_parent) {
        Ok(relative.to_path_buf())
    } else {
        Ok(included_path.to_path_buf())
    }
}

fn load_git_config(path: PathBuf) -> Result<GitConfig> {
    let raw = fs::read_to_string(path)?;
    Ok(toml::from_str::<GitConfigDocument>(&raw)?.git)
}

fn load_helm_config(path: PathBuf) -> Result<HelmConfig> {
    let raw = fs::read_to_string(path)?;
    Ok(toml::from_str::<HelmConfigDocument>(&raw)?.helm)
}

fn load_docker_config(path: PathBuf) -> Result<DockerConfig> {
    let raw = fs::read_to_string(path)?;
    Ok(toml::from_str::<DockerConfigDocument>(&raw)?.docker)
}

fn default_output_dir() -> PathBuf {
    PathBuf::from("migration-exports")
}

fn default_recent_run_limit() -> usize {
    10
}

fn default_split_large_transfers() -> bool {
    false
}

fn default_max_transfer_size_mb() -> u64 {
    200
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
    fn parses_and_roundtrips_inline_toml() {
        let raw = r#"
[output]
base_dir = "exports"
recent_run_limit = 5
split_large_transfers = true
max_transfer_size_mb = 150

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
    fn saves_and_loads_inline_config() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("migration-suite.toml");
        let config = AppConfig::default();

        config.save(&path).expect("save");
        let loaded = AppConfig::load(&path).expect("load");

        assert_eq!(config, loaded);
    }

    #[test]
    fn saves_and_loads_split_config() {
        let dir = tempdir().expect("tempdir");
        let root = dir.path().join("migration-suite.toml");
        let layout = ConfigLayout::split_default(&root);
        let config = AppConfig {
            output: OutputConfig {
                base_dir: PathBuf::from("exports"),
                recent_run_limit: 7,
                split_large_transfers: true,
                max_transfer_size_mb: 250,
            },
            git: GitConfig {
                default_branches: vec!["develop".to_string(), "release/abc".to_string()],
                repos: vec![GitRepoConfig {
                    name: "user-api".to_string(),
                    path: PathBuf::from("/tmp/user-api"),
                    remote: Some("origin".to_string()),
                    branches: None,
                    enabled: true,
                }],
            },
            helm: HelmConfig {
                charts: vec![HelmChartConfig {
                    name: "backend".to_string(),
                    reference: "oci://harbor.example.local/charts/backend".to_string(),
                    version: "1.2.3".to_string(),
                    enabled: true,
                }],
            },
            docker: DockerConfig {
                images: vec![DockerImageConfig {
                    name: "user-api".to_string(),
                    repository: "harbor.example.local/apps/user-api".to_string(),
                    tag: "0.3.4-dev".to_string(),
                    enabled: true,
                }],
            },
        };

        config.save_with_layout(&layout).expect("save split");
        let (loaded, loaded_layout) = AppConfig::load_with_layout(&root).expect("load split");

        assert_eq!(config, loaded);
        assert!(loaded_layout.use_split_files);
        assert_eq!(
            fs::read_to_string(root).expect("root"),
            r#"[output]
base_dir = "exports"
recent_run_limit = 7
split_large_transfers = true
max_transfer_size_mb = 250

[includes]
git = "migration-suite.git.toml"
helm = "migration-suite.helm.toml"
docker = "migration-suite.docker.toml"
"#
        );
    }

    #[test]
    fn missing_config_defaults_to_split_layout() {
        let dir = tempdir().expect("tempdir");
        let root = dir.path().join("migration-suite.toml");
        let expected_git = dir.path().join("migration-suite.git.toml");

        let (config, layout) = AppConfig::load_or_default_with_layout(&root).expect("default");

        assert_eq!(config, AppConfig::default());
        assert!(layout.use_split_files);
        assert_eq!(layout.git_path.as_deref(), Some(expected_git.as_path()));
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
