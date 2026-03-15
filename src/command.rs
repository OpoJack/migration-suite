use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use color_eyre::eyre::{Result, bail, eyre};
use tokio::{process::Command, sync::Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandOutput {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
}

impl CommandOutput {
    pub fn success(stdout: impl Into<String>) -> Self {
        Self {
            status: 0,
            stdout: stdout.into(),
            stderr: String::new(),
        }
    }
}

#[async_trait]
pub trait CommandRunner: Send + Sync {
    async fn run(
        &self,
        program: &str,
        args: &[String],
        cwd: Option<&Path>,
    ) -> Result<CommandOutput>;
}

#[derive(Debug, Default)]
pub struct SystemCommandRunner;

#[async_trait]
impl CommandRunner for SystemCommandRunner {
    async fn run(
        &self,
        program: &str,
        args: &[String],
        cwd: Option<&Path>,
    ) -> Result<CommandOutput> {
        let mut command = Command::new(program);
        command.args(args);
        if let Some(cwd) = cwd {
            let cwd = normalize_runtime_path(cwd);
            if !cwd.is_dir() {
                bail!(
                    "working directory does not exist or is not a directory: {}",
                    cwd.display()
                );
            }
            command.current_dir(&cwd);
        }

        let output = command.output().await?;
        Ok(CommandOutput {
            status: output.status.code().unwrap_or(1),
            stdout: String::from_utf8_lossy(&output.stdout).trim().to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        })
    }
}

pub async fn run_checked(
    runner: &dyn CommandRunner,
    program: &str,
    args: &[String],
    cwd: Option<&Path>,
) -> Result<CommandOutput> {
    let output = runner.run(program, args, cwd).await?;
    if output.status != 0 {
        bail!(
            "{program} {} failed with status {}: {}",
            args.join(" "),
            output.status,
            if output.stderr.is_empty() {
                output.stdout.as_str()
            } else {
                output.stderr.as_str()
            }
        );
    }
    Ok(output)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CommandKey {
    pub cwd: Option<PathBuf>,
    pub program: String,
    pub args: Vec<String>,
}

impl CommandKey {
    pub fn new(program: &str, args: &[String], cwd: Option<&Path>) -> Self {
        Self {
            cwd: cwd.map(Path::to_path_buf),
            program: program.to_string(),
            args: args.to_vec(),
        }
    }
}

#[derive(Clone, Default)]
pub struct MockCommandRunner {
    responses: Arc<Mutex<HashMap<CommandKey, CommandOutput>>>,
}

impl MockCommandRunner {
    pub fn with_responses(
        responses: impl IntoIterator<Item = (CommandKey, CommandOutput)>,
    ) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
        }
    }

    pub async fn insert(&self, key: CommandKey, output: CommandOutput) {
        self.responses.lock().await.insert(key, output);
    }
}

#[async_trait]
impl CommandRunner for MockCommandRunner {
    async fn run(
        &self,
        program: &str,
        args: &[String],
        cwd: Option<&Path>,
    ) -> Result<CommandOutput> {
        let key = CommandKey::new(program, args, cwd);
        self.responses
            .lock()
            .await
            .get(&key)
            .cloned()
            .ok_or_else(|| eyre!("no mock response for {:?}", key))
    }
}

fn normalize_runtime_path(path: &Path) -> PathBuf {
    let normalized = replace_control_escapes(&path.to_string_lossy());
    let normalized = convert_msys_path(&normalized);
    PathBuf::from(expand_home_dir(&normalized))
}

fn replace_control_escapes(value: &str) -> String {
    value
        .chars()
        .flat_map(|ch| match ch {
            '\t' => ['\\', 't'].into_iter().collect::<Vec<_>>(),
            '\n' => ['\\', 'n'].into_iter().collect::<Vec<_>>(),
            '\r' => ['\\', 'r'].into_iter().collect::<Vec<_>>(),
            '\u{0008}' => ['\\', 'b'].into_iter().collect::<Vec<_>>(),
            '\u{000C}' => ['\\', 'f'].into_iter().collect::<Vec<_>>(),
            _ => vec![ch],
        })
        .collect()
}

fn expand_home_dir(value: &str) -> String {
    if let Some(stripped) = value
        .strip_prefix("~/")
        .or_else(|| value.strip_prefix("~\\"))
    {
        if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
            return PathBuf::from(home)
                .join(stripped)
                .to_string_lossy()
                .to_string();
        }
    }
    value.to_string()
}

fn convert_msys_path(value: &str) -> String {
    let bytes = value.as_bytes();
    if bytes.len() >= 3
        && bytes[0] == b'/'
        && bytes[1].is_ascii_alphabetic()
        && (bytes[2] == b'/' || bytes[2] == b'\\')
    {
        let drive = (bytes[1] as char).to_ascii_uppercase();
        let rest = value[3..].replace('/', "\\");
        if rest.is_empty() {
            format!("{drive}:\\")
        } else {
            format!("{drive}:\\{rest}")
        }
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_runtime_path_restores_common_toml_escapes() {
        let raw = PathBuf::from("C:\ttools\runtime\nrepo");
        let normalized = normalize_runtime_path(&raw);
        assert_eq!(normalized.to_string_lossy(), r"C:\ttools\runtime\nrepo");
    }

    #[test]
    fn replace_control_escapes_preserves_plain_paths() {
        assert_eq!(replace_control_escapes("/tmp/repo"), "/tmp/repo");
    }

    #[test]
    fn convert_msys_path_turns_git_bash_style_into_windows_path() {
        assert_eq!(
            convert_msys_path("/c/Users/jack/repo"),
            r"C:\Users\jack\repo"
        );
        assert_eq!(convert_msys_path("/d/work"), r"D:\work");
    }
}
