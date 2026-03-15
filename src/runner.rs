use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use color_eyre::eyre::{Result, eyre};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    command::{CommandRunner, run_checked},
    config::{AppConfig, DockerImageConfig, GitRepoConfig, HelmChartConfig},
    manifest::{ArtifactOutput, JobKind, LogEntry, ManifestItem, RunManifest, RunStatus},
    output::{
        base64_encode_file, create_run_workspace, docker_output_name, file_size, git_output_name,
        gzip_file, helm_output_name, load_recent_manifests, sanitize_filename, sha256_file,
        tar_gz_directory, write_log,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeWindowPreset {
    Hours6,
    Day1,
    Weeks2,
    Months1,
    Months3,
}

impl TimeWindowPreset {
    pub const ALL: [Self; 5] = [
        Self::Hours6,
        Self::Day1,
        Self::Weeks2,
        Self::Months1,
        Self::Months3,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            Self::Hours6 => "6 hours",
            Self::Day1 => "1 day",
            Self::Weeks2 => "2 weeks",
            Self::Months1 => "1 month",
            Self::Months3 => "3 months",
        }
    }

    pub fn git_since_spec(&self) -> &'static str {
        match self {
            Self::Hours6 => "6 hours ago",
            Self::Day1 => "1 day ago",
            Self::Weeks2 => "2 weeks ago",
            Self::Months1 => "1 month ago",
            Self::Months3 => "3 months ago",
        }
    }

    pub fn cutoff(&self) -> DateTime<Utc> {
        let now = Utc::now();
        match self {
            Self::Hours6 => now - Duration::hours(6),
            Self::Day1 => now - Duration::days(1),
            Self::Weeks2 => now - Duration::weeks(2),
            Self::Months1 => now - Duration::days(30),
            Self::Months3 => now - Duration::days(90),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitRepoPreview {
    pub name: String,
    pub path: PathBuf,
    pub branches_checked: Vec<String>,
    pub changed_branches: Vec<String>,
    pub tags_in_window: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitSkippedRepo {
    pub name: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitPreview {
    pub preset: TimeWindowPreset,
    pub included: Vec<GitRepoPreview>,
    pub skipped: Vec<GitSkippedRepo>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HelmSelectionPreview {
    pub name: String,
    pub reference: String,
    pub version: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HelmPreview {
    pub charts: Vec<HelmSelectionPreview>,
    pub output_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DockerSelectionPreview {
    pub name: String,
    pub reference: String,
    pub output_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DockerPreview {
    pub images: Vec<DockerSelectionPreview>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreviewData {
    Git(GitPreview),
    Helm(HelmPreview),
    Docker(DockerPreview),
}

impl PreviewData {
    pub fn title(&self) -> &'static str {
        match self {
            Self::Git(_) => "Git",
            Self::Helm(_) => "Helm",
            Self::Docker(_) => "Docker",
        }
    }
}

#[derive(Clone, Debug)]
pub enum JobEvent {
    Started { kind: JobKind, description: String },
    Log(String),
    Finished(RunManifest),
    Failed(String),
}

pub async fn build_git_preview(
    config: &AppConfig,
    repo_indices: &[usize],
    preset: TimeWindowPreset,
    runner: &dyn CommandRunner,
) -> Result<GitPreview> {
    let mut included = Vec::new();
    let mut skipped = Vec::new();
    let cutoff = preset.cutoff();

    for index in repo_indices {
        let Some(repo) = config.git.repos.get(*index) else {
            continue;
        };
        if !repo.enabled {
            skipped.push(GitSkippedRepo {
                name: repo.name.clone(),
                reason: "disabled".to_string(),
            });
            continue;
        }

        sync_git_repo(runner, repo).await?;

        let branches_checked = repo.branches(&config.git.default_branches).to_vec();
        let mut changed_branches = Vec::new();
        for branch in &branches_checked {
            if branch_has_commits_in_window(runner, repo, branch, preset).await? {
                changed_branches.push(branch.clone());
            }
        }

        let tags_in_window = tags_in_window(runner, repo, cutoff).await?;
        if !changed_branches.is_empty() || !tags_in_window.is_empty() {
            included.push(GitRepoPreview {
                name: repo.name.clone(),
                path: repo.path.clone(),
                branches_checked,
                changed_branches,
                tags_in_window,
            });
        } else {
            skipped.push(GitSkippedRepo {
                name: repo.name.clone(),
                reason: "no commits or tags in the selected window".to_string(),
            });
        }
    }

    Ok(GitPreview {
        preset,
        included,
        skipped,
    })
}

pub fn build_helm_preview(config: &AppConfig, chart_indices: &[usize]) -> HelmPreview {
    HelmPreview {
        charts: chart_indices
            .iter()
            .filter_map(|index| config.helm.charts.get(*index))
            .filter(|chart| chart.enabled)
            .map(|chart| HelmSelectionPreview {
                name: chart.name.clone(),
                reference: chart.reference.clone(),
                version: chart.version.clone(),
            })
            .collect(),
        output_name: "helm-charts_<timestamp>.tar.gz.txt".to_string(),
    }
}

pub fn build_docker_preview(config: &AppConfig, image_indices: &[usize]) -> DockerPreview {
    DockerPreview {
        images: image_indices
            .iter()
            .filter_map(|index| config.docker.images.get(*index))
            .filter(|image| image.enabled)
            .map(|image| DockerSelectionPreview {
                name: image.name.clone(),
                reference: format!("{}:{}", image.repository, image.tag),
                output_name: docker_output_name(&image.name, &image.tag),
            })
            .collect(),
    }
}

pub async fn spawn_git_job(
    config: AppConfig,
    preview: GitPreview,
    runner: Arc<dyn CommandRunner>,
    tx: UnboundedSender<JobEvent>,
) {
    tokio::spawn(async move {
        let _ = tx.send(JobEvent::Started {
            kind: JobKind::Git,
            description: format!(
                "Packaging {} repos from the {} window",
                preview.included.len(),
                preview.preset.label()
            ),
        });
        match run_git_export(&config, &preview, runner.as_ref(), &tx).await {
            Ok(manifest) => {
                let _ = tx.send(JobEvent::Finished(manifest));
            }
            Err(error) => {
                let _ = tx.send(JobEvent::Failed(error.to_string()));
            }
        }
    });
}

pub async fn spawn_helm_job(
    config: AppConfig,
    preview: HelmPreview,
    runner: Arc<dyn CommandRunner>,
    tx: UnboundedSender<JobEvent>,
) {
    tokio::spawn(async move {
        let _ = tx.send(JobEvent::Started {
            kind: JobKind::Helm,
            description: format!("Packaging {} helm charts", preview.charts.len()),
        });
        match run_helm_export(&config, &preview, runner.as_ref(), &tx).await {
            Ok(manifest) => {
                let _ = tx.send(JobEvent::Finished(manifest));
            }
            Err(error) => {
                let _ = tx.send(JobEvent::Failed(error.to_string()));
            }
        }
    });
}

pub async fn spawn_docker_job(
    config: AppConfig,
    preview: DockerPreview,
    runner: Arc<dyn CommandRunner>,
    tx: UnboundedSender<JobEvent>,
) {
    tokio::spawn(async move {
        let _ = tx.send(JobEvent::Started {
            kind: JobKind::Docker,
            description: format!("Packaging {} docker images", preview.images.len()),
        });
        match run_docker_export(&config, &preview, runner.as_ref(), &tx).await {
            Ok(manifest) => {
                let _ = tx.send(JobEvent::Finished(manifest));
            }
            Err(error) => {
                let _ = tx.send(JobEvent::Failed(error.to_string()));
            }
        }
    });
}

pub fn recent_runs(config: &AppConfig) -> Result<Vec<RunManifest>> {
    load_recent_manifests(&config.output.base_dir, config.output.recent_run_limit)
}

async fn run_git_export(
    config: &AppConfig,
    preview: &GitPreview,
    runner: &dyn CommandRunner,
    tx: &UnboundedSender<JobEvent>,
) -> Result<RunManifest> {
    if preview.included.is_empty() {
        return Err(eyre!("the Git preview contains no repos to export"));
    }

    let workspace = create_run_workspace(&config.output.base_dir, "git")?;
    let stamp = workspace.run_id.trim_start_matches("git-");
    let git_root = workspace.root_dir.join("git");
    fs::create_dir_all(&git_root)?;

    let started_at = Utc::now();
    let mut log_lines = Vec::new();
    let mut items = Vec::new();

    for repo in &preview.included {
        let repo_config = config
            .git
            .repos
            .iter()
            .find(|entry| entry.name == repo.name && entry.path == repo.path)
            .ok_or_else(|| eyre!("missing git config entry for {}", repo.name))?;
        let remote = git_remote_name(repo_config);
        log(
            tx,
            &mut log_lines,
            format!("Refreshing {} from remote {}", repo.name, remote),
        );
        sync_git_repo(runner, repo_config).await?;
        log(
            tx,
            &mut log_lines,
            format!("Preparing bundle for {}", repo.name),
        );
        let repo_dir = git_root.join(sanitize_filename(&repo.name));
        fs::create_dir_all(&repo_dir)?;
        let bundle_path = repo_dir.join("bundle");

        let mut args = vec![
            "bundle".to_string(),
            "create".to_string(),
            bundle_path.to_string_lossy().to_string(),
        ];

        for branch in &repo.changed_branches {
            args.push(remote_branch_ref(repo_config, branch));
            if let Some(base_commit) =
                latest_commit_before_window(runner, &repo.path, remote, branch, preview.preset)
                    .await?
            {
                args.push(format!("^{base_commit}"));
            }
        }

        for tag in &repo.tags_in_window {
            args.push(format!("refs/tags/{tag}"));
        }

        run_checked(runner, "git", &args, Some(&repo.path)).await?;
        items.push(ManifestItem {
            name: repo.name.clone(),
            item_type: "git_repo".to_string(),
            source: repo.path.display().to_string(),
            detail: format!(
                "remote={} branches={} tags={}",
                remote,
                repo.changed_branches.join(","),
                repo.tags_in_window.join(",")
            ),
            included: true,
        });
        log(
            tx,
            &mut log_lines,
            format!(
                "{} exported with {} changed branches and {} tags",
                repo.name,
                repo.changed_branches.len(),
                repo.tags_in_window.len()
            ),
        );
    }

    let payload_basename = git_output_name(stamp).trim_end_matches(".txt").to_string();
    let tar_gz_path = workspace.root_dir.join(&payload_basename);
    tar_gz_directory(&git_root, &tar_gz_path)?;

    let final_txt_path = workspace.root_dir.join(git_output_name(stamp));
    base64_encode_file(&tar_gz_path, &final_txt_path)?;
    let output = ArtifactOutput {
        label: "git_payload".to_string(),
        path: final_txt_path.clone(),
        sha256: sha256_file(&final_txt_path)?,
        size_bytes: file_size(&final_txt_path)?,
    };

    let finished_at = Utc::now();
    write_log(&workspace.log_path, &log_lines)?;
    let manifest = RunManifest {
        run_id: workspace.run_id,
        kind: JobKind::Git,
        status: RunStatus::Success,
        started_at,
        finished_at,
        output_dir: workspace.root_dir.clone(),
        summary: format!("Exported {} git repos", items.len()),
        notes: vec!["git_lfs_export_not_implemented".to_string()],
        outputs: vec![output],
        items,
        logs: logs_to_entries(&log_lines, finished_at),
    };
    manifest.save(&workspace.manifest_path)?;
    Ok(manifest)
}

async fn run_helm_export(
    config: &AppConfig,
    preview: &HelmPreview,
    runner: &dyn CommandRunner,
    tx: &UnboundedSender<JobEvent>,
) -> Result<RunManifest> {
    if preview.charts.is_empty() {
        return Err(eyre!("the Helm preview contains no charts to export"));
    }

    let workspace = create_run_workspace(&config.output.base_dir, "helm")?;
    let stamp = workspace.run_id.trim_start_matches("helm-");
    let charts_dir = workspace.root_dir.join("helm");
    fs::create_dir_all(&charts_dir)?;

    let started_at = Utc::now();
    let mut log_lines = Vec::new();
    let mut items = Vec::new();

    for chart in &preview.charts {
        log(
            tx,
            &mut log_lines,
            format!("Pulling chart {} {}", chart.name, chart.version),
        );
        let args = vec![
            "pull".to_string(),
            chart.reference.clone(),
            "--version".to_string(),
            chart.version.clone(),
            "--destination".to_string(),
            charts_dir.to_string_lossy().to_string(),
        ];
        run_checked(runner, "helm", &args, None).await?;
        items.push(ManifestItem {
            name: chart.name.clone(),
            item_type: "helm_chart".to_string(),
            source: chart.reference.clone(),
            detail: format!("version={}", chart.version),
            included: true,
        });
    }

    let payload_basename = helm_output_name(stamp).trim_end_matches(".txt").to_string();
    let tar_gz_path = workspace.root_dir.join(&payload_basename);
    tar_gz_directory(&charts_dir, &tar_gz_path)?;

    let final_txt_path = workspace.root_dir.join(helm_output_name(stamp));
    base64_encode_file(&tar_gz_path, &final_txt_path)?;
    let output = ArtifactOutput {
        label: "helm_payload".to_string(),
        path: final_txt_path.clone(),
        sha256: sha256_file(&final_txt_path)?,
        size_bytes: file_size(&final_txt_path)?,
    };

    let finished_at = Utc::now();
    write_log(&workspace.log_path, &log_lines)?;
    let manifest = RunManifest {
        run_id: workspace.run_id,
        kind: JobKind::Helm,
        status: RunStatus::Success,
        started_at,
        finished_at,
        output_dir: workspace.root_dir.clone(),
        summary: format!("Exported {} helm charts", items.len()),
        notes: Vec::new(),
        outputs: vec![output],
        items,
        logs: logs_to_entries(&log_lines, finished_at),
    };
    manifest.save(&workspace.manifest_path)?;
    Ok(manifest)
}

async fn run_docker_export(
    config: &AppConfig,
    preview: &DockerPreview,
    runner: &dyn CommandRunner,
    tx: &UnboundedSender<JobEvent>,
) -> Result<RunManifest> {
    if preview.images.is_empty() {
        return Err(eyre!("the Docker preview contains no images to export"));
    }

    let workspace = create_run_workspace(&config.output.base_dir, "docker")?;
    let docker_dir = workspace.root_dir.join("docker");
    fs::create_dir_all(&docker_dir)?;

    let started_at = Utc::now();
    let mut log_lines = Vec::new();
    let mut items = Vec::new();
    let mut outputs = Vec::new();

    for image in &preview.images {
        log(tx, &mut log_lines, format!("Pulling {}", image.reference));
        run_checked(
            runner,
            "docker",
            &vec!["pull".to_string(), image.reference.clone()],
            None,
        )
        .await?;

        let image_dir = docker_dir.join(sanitize_filename(&image.name));
        fs::create_dir_all(&image_dir)?;
        let tar_path = image_dir.join(format!(
            "{}_{}.tar",
            sanitize_filename(&image.name),
            sanitize_filename(image.reference.split(':').nth(1).unwrap_or("latest"))
        ));
        let save_args = vec![
            "save".to_string(),
            "-o".to_string(),
            tar_path.to_string_lossy().to_string(),
            image.reference.clone(),
        ];
        log(tx, &mut log_lines, format!("Saving {}", image.reference));
        run_checked(runner, "docker", &save_args, None).await?;

        let gz_path = tar_path.with_extension("tar.gz");
        gzip_file(&tar_path, &gz_path)?;

        let txt_path = workspace.root_dir.join(&image.output_name);
        base64_encode_file(&gz_path, &txt_path)?;
        outputs.push(ArtifactOutput {
            label: image.name.clone(),
            path: txt_path.clone(),
            sha256: sha256_file(&txt_path)?,
            size_bytes: file_size(&txt_path)?,
        });
        items.push(ManifestItem {
            name: image.name.clone(),
            item_type: "docker_image".to_string(),
            source: image.reference.clone(),
            detail: format!("output={}", image.output_name),
            included: true,
        });
        log(
            tx,
            &mut log_lines,
            format!("Exported {}", image.output_name),
        );
    }

    let finished_at = Utc::now();
    write_log(&workspace.log_path, &log_lines)?;
    let manifest = RunManifest {
        run_id: workspace.run_id,
        kind: JobKind::Docker,
        status: RunStatus::Success,
        started_at,
        finished_at,
        output_dir: workspace.root_dir.clone(),
        summary: format!("Exported {} docker images", items.len()),
        notes: vec!["docker_exports_run_sequentially".to_string()],
        outputs,
        items,
        logs: logs_to_entries(&log_lines, finished_at),
    };
    manifest.save(&workspace.manifest_path)?;
    Ok(manifest)
}

fn logs_to_entries(lines: &[String], timestamp: DateTime<Utc>) -> Vec<LogEntry> {
    lines
        .iter()
        .map(|message| LogEntry {
            timestamp,
            message: message.clone(),
        })
        .collect()
}

fn log(tx: &UnboundedSender<JobEvent>, lines: &mut Vec<String>, message: String) {
    lines.push(message.clone());
    let _ = tx.send(JobEvent::Log(message));
}

async fn branch_has_commits_in_window(
    runner: &dyn CommandRunner,
    repo: &GitRepoConfig,
    branch: &str,
    preset: TimeWindowPreset,
) -> Result<bool> {
    let verify_args = vec![
        "rev-parse".to_string(),
        "--verify".to_string(),
        remote_branch_ref(repo, branch),
    ];
    let exists = runner.run("git", &verify_args, Some(&repo.path)).await?;
    if exists.status != 0 {
        return Ok(false);
    }

    let args = vec![
        "rev-list".to_string(),
        "--count".to_string(),
        format!("--since={}", preset.git_since_spec()),
        remote_branch_ref(repo, branch),
    ];
    let output = run_checked(runner, "git", &args, Some(&repo.path)).await?;
    Ok(output.stdout.trim().parse::<u64>().unwrap_or(0) > 0)
}

async fn latest_commit_before_window(
    runner: &dyn CommandRunner,
    repo_path: &Path,
    remote: &str,
    branch: &str,
    preset: TimeWindowPreset,
) -> Result<Option<String>> {
    let args = vec![
        "rev-list".to_string(),
        "-n".to_string(),
        "1".to_string(),
        format!("--before={}", preset.git_since_spec()),
        remote_branch_ref_from_name(remote, branch),
    ];
    let output = run_checked(runner, "git", &args, Some(repo_path)).await?;
    let commit = output.stdout.trim();
    if commit.is_empty() {
        Ok(None)
    } else {
        Ok(Some(commit.to_string()))
    }
}

async fn tags_in_window(
    runner: &dyn CommandRunner,
    repo: &GitRepoConfig,
    cutoff: DateTime<Utc>,
) -> Result<Vec<String>> {
    let args = vec![
        "for-each-ref".to_string(),
        "refs/tags".to_string(),
        "--format=%(refname:strip=2)\t%(creatordate:iso-strict)".to_string(),
    ];
    let output = run_checked(runner, "git", &args, Some(&repo.path)).await?;
    let mut tags = Vec::new();
    for line in output.stdout.lines() {
        let mut parts = line.split('\t');
        let Some(name) = parts.next() else {
            continue;
        };
        let Some(date) = parts.next() else {
            continue;
        };
        let Ok(parsed) = DateTime::parse_from_rfc3339(date) else {
            continue;
        };
        if parsed.with_timezone(&Utc) >= cutoff {
            tags.push(name.to_string());
        }
    }
    Ok(tags)
}

async fn sync_git_repo(runner: &dyn CommandRunner, repo: &GitRepoConfig) -> Result<()> {
    let args = vec![
        "fetch".to_string(),
        "--prune".to_string(),
        "--tags".to_string(),
        git_remote_name(repo).to_string(),
    ];
    run_checked(runner, "git", &args, Some(&repo.path)).await?;
    Ok(())
}

fn git_remote_name(repo: &GitRepoConfig) -> &str {
    repo.remote.as_deref().unwrap_or("origin")
}

fn remote_branch_ref(repo: &GitRepoConfig, branch: &str) -> String {
    remote_branch_ref_from_name(git_remote_name(repo), branch)
}

fn remote_branch_ref_from_name(remote: &str, branch: &str) -> String {
    format!("refs/remotes/{remote}/{branch}")
}

pub fn selected_git_repo_indices(config: &AppConfig, selected: &[bool]) -> Vec<usize> {
    config
        .git
        .repos
        .iter()
        .enumerate()
        .filter_map(|(index, repo)| {
            selected
                .get(index)
                .copied()
                .unwrap_or(repo.enabled)
                .then_some(index)
        })
        .collect()
}

pub fn selected_helm_chart_indices(config: &AppConfig, selected: &[bool]) -> Vec<usize> {
    config
        .helm
        .charts
        .iter()
        .enumerate()
        .filter_map(|(index, chart)| {
            selected
                .get(index)
                .copied()
                .unwrap_or(chart.enabled)
                .then_some(index)
        })
        .collect()
}

pub fn selected_docker_image_indices(config: &AppConfig, selected: &[bool]) -> Vec<usize> {
    config
        .docker
        .images
        .iter()
        .enumerate()
        .filter_map(|(index, image)| {
            selected
                .get(index)
                .copied()
                .unwrap_or(image.enabled)
                .then_some(index)
        })
        .collect()
}

pub fn helm_preview_from_items(charts: &[HelmChartConfig]) -> HelmPreview {
    HelmPreview {
        charts: charts
            .iter()
            .map(|chart| HelmSelectionPreview {
                name: chart.name.clone(),
                reference: chart.reference.clone(),
                version: chart.version.clone(),
            })
            .collect(),
        output_name: "helm-charts_<timestamp>.tar.gz.txt".to_string(),
    }
}

pub fn docker_preview_from_items(images: &[DockerImageConfig]) -> DockerPreview {
    DockerPreview {
        images: images
            .iter()
            .map(|image| DockerSelectionPreview {
                name: image.name.clone(),
                reference: format!("{}:{}", image.repository, image.tag),
                output_name: docker_output_name(&image.name, &image.tag),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin};

    use async_trait::async_trait;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        command::{CommandKey, CommandOutput, MockCommandRunner},
        config::{DockerConfig, GitConfig, OutputConfig},
    };

    #[tokio::test]
    async fn git_preview_detects_commit_hit_on_default_branch() {
        let repo_path = PathBuf::from("/tmp/user-api");
        let config = AppConfig {
            output: OutputConfig::default(),
            git: GitConfig {
                default_branches: vec!["develop".to_string()],
                repos: vec![GitRepoConfig {
                    name: "user-api".to_string(),
                    path: repo_path.clone(),
                    remote: None,
                    branches: None,
                    enabled: true,
                }],
            },
            helm: Default::default(),
            docker: Default::default(),
        };
        let runner = MockCommandRunner::with_responses(vec![
            (
                CommandKey::new(
                    "git",
                    &[
                        "fetch".to_string(),
                        "--prune".to_string(),
                        "--tags".to_string(),
                        "origin".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success(""),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "rev-parse".to_string(),
                        "--verify".to_string(),
                        "refs/remotes/origin/develop".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success("abc123"),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "rev-list".to_string(),
                        "--count".to_string(),
                        "--since=2 weeks ago".to_string(),
                        "refs/remotes/origin/develop".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success("3"),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "for-each-ref".to_string(),
                        "refs/tags".to_string(),
                        "--format=%(refname:strip=2)\t%(creatordate:iso-strict)".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success(""),
            ),
        ]);

        let preview = build_git_preview(&config, &[0], TimeWindowPreset::Weeks2, &runner)
            .await
            .expect("preview");

        assert_eq!(preview.included.len(), 1);
        assert_eq!(preview.included[0].changed_branches, vec!["develop"]);
    }

    #[tokio::test]
    async fn git_preview_detects_release_branch_and_tags() {
        let repo_path = PathBuf::from("/tmp/auth-service");
        let cutoff = Utc::now().format("%+").to_string();
        let config = AppConfig {
            output: OutputConfig::default(),
            git: GitConfig {
                default_branches: vec!["develop".to_string()],
                repos: vec![GitRepoConfig {
                    name: "auth-service".to_string(),
                    path: repo_path.clone(),
                    remote: None,
                    branches: Some(vec!["develop".to_string(), "release/abc".to_string()]),
                    enabled: true,
                }],
            },
            helm: Default::default(),
            docker: Default::default(),
        };
        let runner = MockCommandRunner::with_responses(vec![
            (
                CommandKey::new(
                    "git",
                    &[
                        "fetch".to_string(),
                        "--prune".to_string(),
                        "--tags".to_string(),
                        "origin".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success(""),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "rev-parse".to_string(),
                        "--verify".to_string(),
                        "refs/remotes/origin/develop".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success("base"),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "rev-list".to_string(),
                        "--count".to_string(),
                        "--since=2 weeks ago".to_string(),
                        "refs/remotes/origin/develop".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success("0"),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "rev-parse".to_string(),
                        "--verify".to_string(),
                        "refs/remotes/origin/release/abc".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success("head"),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "rev-list".to_string(),
                        "--count".to_string(),
                        "--since=2 weeks ago".to_string(),
                        "refs/remotes/origin/release/abc".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success("2"),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "for-each-ref".to_string(),
                        "refs/tags".to_string(),
                        "--format=%(refname:strip=2)\t%(creatordate:iso-strict)".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success(format!("v1.2.3\t{cutoff}")),
            ),
        ]);

        let preview = build_git_preview(&config, &[0], TimeWindowPreset::Weeks2, &runner)
            .await
            .expect("preview");

        assert_eq!(preview.included[0].changed_branches, vec!["release/abc"]);
        assert_eq!(preview.included[0].tags_in_window, vec!["v1.2.3"]);
    }

    #[tokio::test]
    async fn git_preview_skips_repo_without_hits() {
        let repo_path = PathBuf::from("/tmp/noop");
        let config = AppConfig {
            output: OutputConfig::default(),
            git: GitConfig {
                default_branches: vec!["develop".to_string()],
                repos: vec![GitRepoConfig {
                    name: "noop".to_string(),
                    path: repo_path.clone(),
                    remote: None,
                    branches: None,
                    enabled: true,
                }],
            },
            helm: Default::default(),
            docker: Default::default(),
        };
        let runner = MockCommandRunner::with_responses(vec![
            (
                CommandKey::new(
                    "git",
                    &[
                        "fetch".to_string(),
                        "--prune".to_string(),
                        "--tags".to_string(),
                        "origin".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success(""),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "rev-parse".to_string(),
                        "--verify".to_string(),
                        "refs/remotes/origin/develop".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success("base"),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "rev-list".to_string(),
                        "--count".to_string(),
                        "--since=2 weeks ago".to_string(),
                        "refs/remotes/origin/develop".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success("0"),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "for-each-ref".to_string(),
                        "refs/tags".to_string(),
                        "--format=%(refname:strip=2)\t%(creatordate:iso-strict)".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success(""),
            ),
        ]);

        let preview = build_git_preview(&config, &[0], TimeWindowPreset::Weeks2, &runner)
            .await
            .expect("preview");

        assert!(preview.included.is_empty());
        assert_eq!(preview.skipped[0].name, "noop");
    }

    #[tokio::test]
    async fn git_preview_uses_configured_remote_name() {
        let repo_path = PathBuf::from("/tmp/payments");
        let config = AppConfig {
            output: OutputConfig::default(),
            git: GitConfig {
                default_branches: vec!["develop".to_string()],
                repos: vec![GitRepoConfig {
                    name: "payments".to_string(),
                    path: repo_path.clone(),
                    remote: Some("company".to_string()),
                    branches: None,
                    enabled: true,
                }],
            },
            helm: Default::default(),
            docker: Default::default(),
        };
        let runner = MockCommandRunner::with_responses(vec![
            (
                CommandKey::new(
                    "git",
                    &[
                        "fetch".to_string(),
                        "--prune".to_string(),
                        "--tags".to_string(),
                        "company".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success(""),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "rev-parse".to_string(),
                        "--verify".to_string(),
                        "refs/remotes/company/develop".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success("abc123"),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "rev-list".to_string(),
                        "--count".to_string(),
                        "--since=2 weeks ago".to_string(),
                        "refs/remotes/company/develop".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success("1"),
            ),
            (
                CommandKey::new(
                    "git",
                    &[
                        "for-each-ref".to_string(),
                        "refs/tags".to_string(),
                        "--format=%(refname:strip=2)\t%(creatordate:iso-strict)".to_string(),
                    ],
                    Some(&repo_path),
                ),
                CommandOutput::success(""),
            ),
        ]);

        let preview = build_git_preview(&config, &[0], TimeWindowPreset::Weeks2, &runner)
            .await
            .expect("preview");

        assert_eq!(preview.included.len(), 1);
        assert_eq!(preview.included[0].changed_branches, vec!["develop"]);
    }

    #[test]
    fn selection_helpers_choose_checked_items() {
        let config = AppConfig {
            output: OutputConfig::default(),
            git: GitConfig {
                default_branches: vec!["develop".to_string()],
                repos: vec![
                    GitRepoConfig {
                        name: "a".to_string(),
                        path: PathBuf::from("/tmp/a"),
                        remote: None,
                        branches: None,
                        enabled: true,
                    },
                    GitRepoConfig {
                        name: "b".to_string(),
                        path: PathBuf::from("/tmp/b"),
                        remote: None,
                        branches: None,
                        enabled: true,
                    },
                ],
            },
            helm: crate::config::HelmConfig {
                charts: vec![
                    HelmChartConfig {
                        name: "c".to_string(),
                        reference: "oci://chart".to_string(),
                        version: "1.0.0".to_string(),
                        enabled: true,
                    },
                    HelmChartConfig {
                        name: "d".to_string(),
                        reference: "oci://chart2".to_string(),
                        version: "2.0.0".to_string(),
                        enabled: false,
                    },
                ],
            },
            docker: DockerConfig {
                images: vec![
                    DockerImageConfig {
                        name: "user-api".to_string(),
                        repository: "harbor/app/user-api".to_string(),
                        tag: "1.0.0".to_string(),
                        enabled: true,
                    },
                    DockerImageConfig {
                        name: "auth-service".to_string(),
                        repository: "harbor/app/auth".to_string(),
                        tag: "2.0.0".to_string(),
                        enabled: true,
                    },
                ],
            },
        };

        assert_eq!(selected_git_repo_indices(&config, &[true, false]), vec![0]);
        assert_eq!(
            selected_helm_chart_indices(&config, &[false, true]),
            vec![1]
        );
        assert_eq!(
            selected_docker_image_indices(&config, &[false, true]),
            vec![1]
        );
    }

    #[tokio::test]
    async fn docker_runner_creates_outputs_and_manifest() {
        let temp = tempdir().expect("tempdir");
        let config = AppConfig {
            output: OutputConfig {
                base_dir: temp.path().join("exports"),
                recent_run_limit: 5,
            },
            git: GitConfig::default(),
            helm: Default::default(),
            docker: DockerConfig {
                images: vec![DockerImageConfig {
                    name: "user-api".to_string(),
                    repository: "harbor/apps/user-api".to_string(),
                    tag: "0.3.4-dev".to_string(),
                    enabled: true,
                }],
            },
        };
        let preview = build_docker_preview(&config, &[0]);
        let runner = ScriptedRunner::new(move |program, args, _cwd| {
            let program = program.to_string();
            let args = args.to_vec();
            Box::pin(async move {
                match (program.as_str(), args.first().map(String::as_str)) {
                    ("docker", Some("pull")) => Ok(CommandOutput::success("pulled")),
                    ("docker", Some("save")) => {
                        let path = PathBuf::from(args[2].clone());
                        fs::write(path, b"docker image bytes")?;
                        Ok(CommandOutput::success("saved"))
                    }
                    _ => Err(eyre!("unexpected command")),
                }
            })
        });
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let manifest = run_docker_export(&config, &preview, &runner, &tx)
            .await
            .expect("docker export");

        assert_eq!(manifest.outputs.len(), 1);
        assert!(manifest.outputs[0].path.exists());
        assert!(config.output.base_dir.exists());
    }

    struct ScriptedRunner {
        handler: Arc<
            dyn Fn(
                    &str,
                    &[String],
                    Option<&Path>,
                ) -> Pin<Box<dyn Future<Output = Result<CommandOutput>> + Send>>
                + Send
                + Sync,
        >,
    }

    impl ScriptedRunner {
        fn new<F>(handler: F) -> Self
        where
            F: Fn(
                    &str,
                    &[String],
                    Option<&Path>,
                ) -> Pin<Box<dyn Future<Output = Result<CommandOutput>> + Send>>
                + Send
                + Sync
                + 'static,
        {
            Self {
                handler: Arc::new(handler),
            }
        }
    }

    #[async_trait]
    impl CommandRunner for ScriptedRunner {
        async fn run(
            &self,
            program: &str,
            args: &[String],
            cwd: Option<&Path>,
        ) -> Result<CommandOutput> {
            (self.handler)(program, args, cwd).await
        }
    }
}
