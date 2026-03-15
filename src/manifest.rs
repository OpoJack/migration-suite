use std::{
    fs,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use color_eyre::eyre::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Git,
    Helm,
    Docker,
}

impl JobKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Git => "git",
            Self::Helm => "helm",
            Self::Docker => "docker",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Success,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactOutput {
    pub label: String,
    pub path: PathBuf,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestItem {
    pub name: String,
    pub item_type: String,
    pub source: String,
    pub detail: String,
    #[serde(default)]
    pub included: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunManifest {
    pub run_id: String,
    pub kind: JobKind,
    pub status: RunStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub output_dir: PathBuf,
    pub summary: String,
    pub notes: Vec<String>,
    pub outputs: Vec<ArtifactOutput>,
    pub items: Vec<ManifestItem>,
    pub logs: Vec<LogEntry>,
}

impl RunManifest {
    pub fn save(&self, path: &Path) -> Result<()> {
        let raw = serde_json::to_string_pretty(self)?;
        fs::write(path, raw)?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&raw)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn manifest_serializes_and_loads() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("manifest.json");
        let manifest = RunManifest {
            run_id: "git-20260101T010101Z".to_string(),
            kind: JobKind::Git,
            status: RunStatus::Success,
            started_at: Utc::now(),
            finished_at: Utc::now(),
            output_dir: dir.path().to_path_buf(),
            summary: "2 repos exported".to_string(),
            notes: vec!["lfs_not_implemented".to_string()],
            outputs: vec![ArtifactOutput {
                label: "payload".to_string(),
                path: dir.path().join("Git-migration_test.tar.gz.txt"),
                sha256: "abc123".to_string(),
                size_bytes: 42,
            }],
            items: vec![ManifestItem {
                name: "user-api".to_string(),
                item_type: "git_repo".to_string(),
                source: "/tmp/user-api".to_string(),
                detail: "branches=develop".to_string(),
                included: true,
            }],
            logs: vec![LogEntry {
                timestamp: Utc::now(),
                message: "done".to_string(),
            }],
        };

        manifest.save(&path).expect("save");
        let decoded = RunManifest::load(&path).expect("load");
        assert_eq!(manifest, decoded);
    }
}
