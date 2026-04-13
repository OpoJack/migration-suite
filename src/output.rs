use std::{
    env,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use base64::{engine::general_purpose::STANDARD, write::EncoderWriter};
use chrono::Utc;
use color_eyre::eyre::Result;
use flate2::{Compression, write::GzEncoder};
use sha2::{Digest, Sha256};
use tar::Builder;

use crate::manifest::{ArtifactOutput, RunManifest, RunStatus};

const IN_PROGRESS_DIR: &str = ".in_progress";
const SUCCESS_DIR: &str = "success";
const FAILED_DIR: &str = "failed";

#[derive(Clone, Debug)]
pub struct RunWorkspace {
    pub base_dir: PathBuf,
    pub run_id: String,
    pub root_dir: PathBuf,
    pub manifest_path: PathBuf,
    pub log_path: PathBuf,
}

pub fn timestamp_slug() -> String {
    Utc::now().format("%Y-%m-%d_%H-%M-%S").to_string()
}

pub fn create_run_workspace(base_dir: &Path, kind: &str) -> Result<RunWorkspace> {
    let base_dir = resolve_base_dir(base_dir)?;
    let pending_root = base_dir.join(IN_PROGRESS_DIR);
    fs::create_dir_all(&pending_root)?;
    let stamp = timestamp_slug();
    let run_id = unique_run_id(&pending_root, &format!("{kind}-{stamp}"));
    let root_dir = pending_root.join(&run_id);
    fs::create_dir_all(&root_dir)?;
    Ok(RunWorkspace {
        base_dir,
        run_id,
        manifest_path: root_dir.join("manifest.json"),
        log_path: root_dir.join("job.log"),
        root_dir,
    })
}

pub fn finalize_run_workspace(workspace: &RunWorkspace, status: RunStatus) -> Result<RunWorkspace> {
    let (folder, dir_name) = match status {
        RunStatus::Success => (SUCCESS_DIR, workspace.run_id.clone()),
        RunStatus::Failed => (FAILED_DIR, format!("{}-failed", workspace.run_id)),
    };
    let target_parent = workspace.base_dir.join(folder);
    fs::create_dir_all(&target_parent)?;
    let target_root = target_parent.join(dir_name);
    fs::rename(&workspace.root_dir, &target_root)?;
    Ok(RunWorkspace {
        base_dir: workspace.base_dir.clone(),
        run_id: workspace.run_id.clone(),
        manifest_path: target_root.join("manifest.json"),
        log_path: target_root.join("job.log"),
        root_dir: target_root,
    })
}

pub fn relocate_artifact_outputs(
    outputs: &[ArtifactOutput],
    old_root: &Path,
    new_root: &Path,
) -> Vec<ArtifactOutput> {
    outputs
        .iter()
        .cloned()
        .map(|mut output| {
            if let Ok(relative) = output.path.strip_prefix(old_root) {
                output.path = new_root.join(relative);
            }
            output
        })
        .collect()
}

pub fn tar_gz_directory(src_dir: &Path, dest_file: &Path) -> Result<()> {
    let file = File::create(dest_file)?;
    let encoder = GzEncoder::new(file, Compression::default());
    let mut archive = Builder::new(encoder);
    archive.append_dir_all(".", src_dir)?;
    archive.finish()?;
    Ok(())
}

pub fn gzip_file(src_file: &Path, dest_file: &Path) -> Result<()> {
    let mut reader = BufReader::new(File::open(src_file)?);
    let writer = File::create(dest_file)?;
    let mut encoder = GzEncoder::new(writer, Compression::default());
    io::copy(&mut reader, &mut encoder)?;
    encoder.finish()?;
    Ok(())
}

pub fn base64_encode_file(src_file: &Path, dest_file: &Path) -> Result<()> {
    let mut reader = BufReader::new(File::open(src_file)?);
    let writer = BufWriter::new(File::create(dest_file)?);
    let mut encoder = EncoderWriter::new(writer, &STANDARD);
    io::copy(&mut reader, &mut encoder)?;
    encoder.finish()?;
    Ok(())
}

pub fn split_file(path: &Path, max_bytes: u64) -> Result<Vec<PathBuf>> {
    if file_size(path)? <= max_bytes {
        return Ok(vec![path.to_path_buf()]);
    }

    let mut reader = BufReader::new(File::open(path)?);
    let mut parts = Vec::new();
    let mut part_index = 1usize;

    loop {
        let part_path = split_part_path(path, part_index);
        let mut writer = BufWriter::new(File::create(&part_path)?);
        let mut remaining = max_bytes;
        let mut wrote_any = false;
        let mut buffer = [0u8; 8192];

        while remaining > 0 {
            let chunk_len = remaining.min(buffer.len() as u64) as usize;
            let read = reader.read(&mut buffer[..chunk_len])?;
            if read == 0 {
                break;
            }
            writer.write_all(&buffer[..read])?;
            remaining -= read as u64;
            wrote_any = true;
        }

        writer.flush()?;
        if !wrote_any {
            fs::remove_file(&part_path)?;
            break;
        }

        parts.push(part_path);
        part_index += 1;
    }

    fs::remove_file(path)?;
    Ok(parts)
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let mut file = BufReader::new(File::open(path)?);
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

pub fn file_size(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path)?.len())
}

pub fn write_log(path: &Path, lines: &[String]) -> Result<()> {
    let mut file = BufWriter::new(File::create(path)?);
    for line in lines {
        writeln!(file, "{line}")?;
    }
    Ok(())
}

pub fn load_recent_manifests(base_dir: &Path, limit: usize) -> Result<Vec<RunManifest>> {
    let base_dir = resolve_base_dir(base_dir)?;
    if !base_dir.exists() {
        return Ok(Vec::new());
    }

    let mut manifests = Vec::new();
    for search_dir in manifest_search_dirs(&base_dir)? {
        for entry in fs::read_dir(search_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let manifest_path = entry.path().join("manifest.json");
            if manifest_path.exists() {
                if let Ok(manifest) = RunManifest::load(&manifest_path) {
                    manifests.push(manifest);
                }
            }
        }
    }

    manifests.sort_by(|left, right| right.started_at.cmp(&left.started_at));
    manifests.truncate(limit);
    Ok(manifests)
}

fn resolve_base_dir(base_dir: &Path) -> Result<PathBuf> {
    if base_dir.is_absolute() {
        Ok(base_dir.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(base_dir))
    }
}

fn manifest_search_dirs(base_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = vec![base_dir.to_path_buf()];
    for child in [SUCCESS_DIR, FAILED_DIR] {
        let path = base_dir.join(child);
        if path.exists() && path.is_dir() {
            dirs.push(path);
        }
    }
    Ok(dirs)
}

fn unique_run_id(parent: &Path, base_run_id: &str) -> String {
    let mut candidate = base_run_id.to_string();
    let mut suffix = 2usize;
    while parent.join(&candidate).exists() {
        candidate = format!("{base_run_id}-{suffix}");
        suffix += 1;
    }
    candidate
}

fn split_part_path(path: &Path, part_index: usize) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| "payload".to_string());
    let part_name = if let Some(prefix) = file_name.strip_suffix(".txt") {
        format!("{prefix}.part{part_index:03}.txt")
    } else {
        format!("{file_name}.part{part_index:03}")
    };
    path.with_file_name(part_name)
}

pub fn sanitize_filename(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '-',
        })
        .collect()
}

pub fn docker_output_name(name: &str, tag: &str) -> String {
    format!(
        "{}_{}.tar.gz.txt",
        sanitize_filename(name),
        sanitize_filename(tag)
    )
}

pub fn git_output_name(stamp: &str) -> String {
    format!("Git-migration_{stamp}.tar.gz.txt")
}

pub fn helm_output_name(stamp: &str) -> String {
    format!("helm-charts_{stamp}.tar.gz.txt")
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn output_naming_matches_expected_contract() {
        assert_eq!(
            docker_output_name("user-api", "0.3.4-dev"),
            "user-api_0.3.4-dev.tar.gz.txt"
        );
        assert_eq!(
            git_output_name("2026-03-14_01-01-01"),
            "Git-migration_2026-03-14_01-01-01.tar.gz.txt"
        );
        assert_eq!(
            helm_output_name("2026-03-14_01-01-01"),
            "helm-charts_2026-03-14_01-01-01.tar.gz.txt"
        );
    }

    #[test]
    fn creates_run_workspace_and_hashes_files() {
        let dir = tempdir().expect("tempdir");
        let workspace = create_run_workspace(dir.path(), "git").expect("workspace");
        let sample = workspace.root_dir.join("sample.txt");
        fs::write(&sample, "hello").expect("write");

        let hash = sha256_file(&sample).expect("hash");
        assert_eq!(file_size(&sample).expect("size"), 5);
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn create_run_workspace_resolves_relative_base_dir_to_absolute_path() {
        let workspace = create_run_workspace(Path::new("exports"), "git").expect("workspace");

        assert!(workspace.root_dir.is_absolute());
        assert!(
            workspace.root_dir.ends_with(
                Path::new("exports")
                    .join(".in_progress")
                    .join(&workspace.run_id)
            )
        );
    }

    #[test]
    fn finalize_run_workspace_moves_success_and_failure_into_status_directories() {
        let dir = tempdir().expect("tempdir");
        let workspace = create_run_workspace(dir.path(), "git").expect("workspace");
        let success = finalize_run_workspace(&workspace, RunStatus::Success).expect("success");
        assert!(
            success
                .root_dir
                .ends_with(Path::new("success").join(&workspace.run_id))
        );

        let failed_workspace = create_run_workspace(dir.path(), "helm").expect("workspace");
        let failed = finalize_run_workspace(&failed_workspace, RunStatus::Failed).expect("failed");
        assert!(
            failed
                .root_dir
                .ends_with(Path::new("failed").join(format!("{}-failed", failed_workspace.run_id)))
        );
    }

    #[test]
    fn load_recent_manifests_reads_success_and_failed_directories() {
        let dir = tempdir().expect("tempdir");
        let success_dir = dir.path().join("success").join("git-2026-01-01_01-01-01");
        let failed_dir = dir
            .path()
            .join("failed")
            .join("docker-2026-01-01_01-02-02-failed");
        fs::create_dir_all(&success_dir).expect("success dir");
        fs::create_dir_all(&failed_dir).expect("failed dir");

        let older = RunManifest {
            run_id: "git-2026-01-01_01-01-01".to_string(),
            kind: crate::manifest::JobKind::Git,
            status: RunStatus::Success,
            started_at: chrono::DateTime::parse_from_rfc3339("2026-01-01T01:01:01Z")
                .expect("time")
                .with_timezone(&Utc),
            finished_at: chrono::DateTime::parse_from_rfc3339("2026-01-01T01:05:01Z")
                .expect("time")
                .with_timezone(&Utc),
            output_dir: success_dir.clone(),
            summary: "git ok".to_string(),
            notes: Vec::new(),
            outputs: Vec::new(),
            items: Vec::new(),
            logs: Vec::new(),
        };
        older
            .save(&success_dir.join("manifest.json"))
            .expect("save success manifest");

        let newer = RunManifest {
            run_id: "docker-2026-01-01_01-02-02".to_string(),
            kind: crate::manifest::JobKind::Docker,
            status: RunStatus::Failed,
            started_at: chrono::DateTime::parse_from_rfc3339("2026-01-01T01:02:02Z")
                .expect("time")
                .with_timezone(&Utc),
            finished_at: chrono::DateTime::parse_from_rfc3339("2026-01-01T01:04:02Z")
                .expect("time")
                .with_timezone(&Utc),
            output_dir: failed_dir.clone(),
            summary: "docker failed".to_string(),
            notes: Vec::new(),
            outputs: Vec::new(),
            items: Vec::new(),
            logs: Vec::new(),
        };
        newer
            .save(&failed_dir.join("manifest.json"))
            .expect("save failed manifest");

        let manifests = load_recent_manifests(dir.path(), 10).expect("load manifests");
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].summary, "docker failed");
        assert_eq!(manifests[1].summary, "git ok");
    }

    #[test]
    fn create_run_workspace_uses_unique_directory_when_timestamp_collides() {
        let dir = tempdir().expect("tempdir");
        let pending_root = dir.path().join(".in_progress");
        fs::create_dir_all(&pending_root).expect("pending root");
        fs::create_dir_all(pending_root.join("git-2026-01-01_01-01-01")).expect("existing run dir");

        let unique = unique_run_id(&pending_root, "git-2026-01-01_01-01-01");
        assert_eq!(unique, "git-2026-01-01_01-01-01-2");
    }

    #[test]
    fn encodes_file_to_base64() {
        let dir = tempdir().expect("tempdir");
        let src = dir.path().join("plain.txt");
        let dest = dir.path().join("plain.txt.b64");
        fs::write(&src, "hello").expect("write");

        base64_encode_file(&src, &dest).expect("base64");
        let encoded = fs::read_to_string(dest).expect("read");

        assert_eq!(encoded, STANDARD.encode("hello"));
    }

    #[test]
    fn split_file_creates_numbered_parts_and_removes_original() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("bundle.tar.gz.txt");
        fs::write(&path, vec![b'x'; 11]).expect("write");

        let parts = split_file(&path, 4).expect("split");

        assert_eq!(parts.len(), 3);
        assert_eq!(
            parts
                .iter()
                .map(|part| part.file_name().unwrap().to_string_lossy().to_string())
                .collect::<Vec<_>>(),
            vec![
                "bundle.tar.gz.part001.txt".to_string(),
                "bundle.tar.gz.part002.txt".to_string(),
                "bundle.tar.gz.part003.txt".to_string(),
            ]
        );
        assert_eq!(file_size(&parts[0]).expect("part 1 size"), 4);
        assert_eq!(file_size(&parts[1]).expect("part 2 size"), 4);
        assert_eq!(file_size(&parts[2]).expect("part 3 size"), 3);
        assert!(!path.exists());
    }
}
