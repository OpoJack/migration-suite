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

use crate::manifest::RunManifest;

#[derive(Clone, Debug)]
pub struct RunWorkspace {
    pub run_id: String,
    pub root_dir: PathBuf,
    pub manifest_path: PathBuf,
    pub log_path: PathBuf,
}

pub fn timestamp_slug() -> String {
    Utc::now().format("%Y%m%dT%H%M%SZ").to_string()
}

pub fn create_run_workspace(base_dir: &Path, kind: &str) -> Result<RunWorkspace> {
    let base_dir = resolve_base_dir(base_dir)?;
    fs::create_dir_all(&base_dir)?;
    let stamp = timestamp_slug();
    let run_id = format!("{kind}-{stamp}");
    let root_dir = base_dir.join(&run_id);
    fs::create_dir_all(&root_dir)?;
    Ok(RunWorkspace {
        run_id,
        manifest_path: root_dir.join("manifest.json"),
        log_path: root_dir.join("job.log"),
        root_dir,
    })
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
    for entry in fs::read_dir(base_dir)? {
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
            git_output_name("20260314T010101Z"),
            "Git-migration_20260314T010101Z.tar.gz.txt"
        );
        assert_eq!(
            helm_output_name("20260314T010101Z"),
            "helm-charts_20260314T010101Z.tar.gz.txt"
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
            workspace
                .root_dir
                .ends_with(Path::new("exports").join(&workspace.run_id))
        );
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
}
