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
            command.current_dir(cwd);
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
