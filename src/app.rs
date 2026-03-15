use std::{env, path::PathBuf, sync::Arc};

use color_eyre::eyre::{Result, eyre};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures::StreamExt;
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Tabs, Wrap},
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::{
    command::{CommandRunner, SystemCommandRunner},
    config::{
        AppConfig, ConfigLayout, DockerImageConfig, GitRepoConfig, HelmChartConfig,
        branches_to_csv, config_path_from_cwd, csv_to_branches,
    },
    manifest::{JobKind, RunManifest, RunStatus},
    runner::{
        DockerPreview, GitPreview, HelmPreview, JobEvent, PreviewData, TimeWindowPreset,
        build_docker_preview, build_helm_preview, recent_runs, selected_docker_image_indices,
        selected_git_repo_indices, selected_helm_chart_indices, spawn_docker_job, spawn_git_job,
        spawn_git_preview_generation, spawn_helm_job,
    },
};

pub struct App {
    running: bool,
    event_stream: EventStream,
    runner: Arc<dyn CommandRunner>,
    config_path: PathBuf,
    config_layout: ConfigLayout,
    config: AppConfig,
    recent_runs: Vec<RunManifest>,
    active_tab: MainTab,
    status_message: String,
    status_indicator_visible: bool,
    config_dirty: bool,
    git_selected: Vec<bool>,
    helm_selected: Vec<bool>,
    docker_selected: Vec<bool>,
    git_cursor: usize,
    helm_cursor: usize,
    docker_cursor: usize,
    jobs_cursor: usize,
    config_focus: ConfigSection,
    config_cursor: usize,
    git_preset_index: usize,
    preview_modal: Option<PreviewModal>,
    last_preview: Option<PreviewData>,
    preview_generation_active: bool,
    form_modal: Option<FormModal>,
    job_sender: UnboundedSender<JobEvent>,
    job_receiver: UnboundedReceiver<JobEvent>,
    current_job: Option<CurrentJob>,
}

impl App {
    pub fn bootstrap() -> Result<Self> {
        let cwd = env::current_dir()?;
        let config_path = config_path_from_cwd(&cwd);
        let (config, config_layout) = AppConfig::load_or_default_with_layout(&config_path)?;
        let recent_runs = recent_runs(&config).unwrap_or_default();
        let (job_sender, job_receiver) = unbounded_channel();

        Ok(Self {
            running: true,
            event_stream: EventStream::default(),
            runner: Arc::new(SystemCommandRunner),
            config_path,
            config_layout,
            git_selected: config.git.repos.iter().map(|repo| repo.enabled).collect(),
            helm_selected: config
                .helm
                .charts
                .iter()
                .map(|chart| chart.enabled)
                .collect(),
            docker_selected: config
                .docker
                .images
                .iter()
                .map(|image| image.enabled)
                .collect(),
            config,
            recent_runs,
            active_tab: MainTab::Git,
            status_message: "Loaded configuration".to_string(),
            status_indicator_visible: false,
            config_dirty: false,
            git_cursor: 0,
            helm_cursor: 0,
            docker_cursor: 0,
            jobs_cursor: 0,
            config_focus: ConfigSection::Output,
            config_cursor: 0,
            git_preset_index: 2,
            preview_modal: None,
            last_preview: None,
            preview_generation_active: false,
            form_modal: None,
            job_sender,
            job_receiver,
            current_job: None,
        })
    }

    pub async fn run(mut self, mut terminal: DefaultTerminal) -> Result<()> {
        while self.running {
            terminal.draw(|frame| self.draw(frame))?;
            tokio::select! {
                maybe_event = self.event_stream.next() => {
                    if let Some(Ok(event)) = maybe_event {
                        if let Err(error) = self.handle_event(event).await {
                            self.status_message = error.to_string();
                        }
                    }
                }
                maybe_job = self.job_receiver.recv() => {
                    if let Some(job) = maybe_job {
                        self.handle_job_event(job);
                    }
                }
            }
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(10),
                Constraint::Length(3),
            ])
            .split(frame.area());

        self.draw_tabs(frame, layout[0]);
        match self.active_tab {
            MainTab::Git => self.draw_git_tab(frame, layout[1]),
            MainTab::Helm => self.draw_helm_tab(frame, layout[1]),
            MainTab::Docker => self.draw_docker_tab(frame, layout[1]),
            MainTab::Jobs => self.draw_jobs_tab(frame, layout[1]),
            MainTab::Config => self.draw_config_tab(frame, layout[1]),
        }
        self.draw_footer(frame, layout[2]);

        if let Some(preview) = self.preview_modal.as_ref() {
            self.draw_preview_modal(frame, preview);
        }
        if let Some(form) = self.form_modal.as_ref() {
            self.draw_form_modal(frame, form);
        }
    }

    async fn handle_event(&mut self, event: Event) -> Result<()> {
        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => self.on_key_event(key).await?,
            Event::Resize(_, _) | Event::Mouse(_) => {}
            _ => {}
        }
        Ok(())
    }

    async fn on_key_event(&mut self, key: KeyEvent) -> Result<()> {
        if matches!(
            (key.modifiers, key.code),
            (
                KeyModifiers::CONTROL,
                KeyCode::Char('c') | KeyCode::Char('C')
            )
        ) {
            self.request_quit()?;
            return Ok(());
        }

        if self.form_modal.is_some() {
            self.handle_form_key(key)?;
            return Ok(());
        }

        if self.preview_modal.is_some() {
            return self.handle_preview_key(key).await;
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => self.request_quit()?,
            KeyCode::Tab => self.set_active_tab(self.active_tab.next()),
            KeyCode::BackTab => self.set_active_tab(self.active_tab.previous()),
            _ => match self.active_tab {
                MainTab::Git => self.handle_git_key(key).await?,
                MainTab::Helm => self.handle_helm_key(key)?,
                MainTab::Docker => self.handle_docker_key(key)?,
                MainTab::Jobs => self.handle_jobs_key(key)?,
                MainTab::Config => self.handle_config_key(key)?,
            },
        }
        Ok(())
    }

    async fn handle_git_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Up => self.git_cursor = self.git_cursor.saturating_sub(1),
            KeyCode::Down => {
                if self.git_cursor + 1 < self.config.git.repos.len() {
                    self.git_cursor += 1;
                }
            }
            KeyCode::Char(' ') => {
                if let Some(selected) = self.git_selected.get_mut(self.git_cursor) {
                    *selected = !*selected;
                }
                self.sync_git_selection_state();
            }
            KeyCode::Char('a') => {
                toggle_all(&mut self.git_selected);
                self.sync_git_selection_state();
            }
            KeyCode::Left => {
                self.git_preset_index = self.git_preset_index.saturating_sub(1);
            }
            KeyCode::Right => {
                if self.git_preset_index + 1 < TimeWindowPreset::ALL.len() {
                    self.git_preset_index += 1;
                }
            }
            KeyCode::Char('p') => {
                self.open_git_preview_confirmation()?;
            }
            KeyCode::Char('r') => {
                self.open_git_preview_confirmation()?;
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_helm_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Up => self.helm_cursor = self.helm_cursor.saturating_sub(1),
            KeyCode::Down => {
                if self.helm_cursor + 1 < self.config.helm.charts.len() {
                    self.helm_cursor += 1;
                }
            }
            KeyCode::Char(' ') => {
                if let Some(selected) = self.helm_selected.get_mut(self.helm_cursor) {
                    *selected = !*selected;
                }
                self.sync_helm_selection_state();
            }
            KeyCode::Char('a') => {
                toggle_all(&mut self.helm_selected);
                self.sync_helm_selection_state();
            }
            KeyCode::Char('p') | KeyCode::Char('r') => {
                let preview = build_helm_preview(
                    &self.config,
                    &selected_helm_chart_indices(&self.config, &self.helm_selected),
                );
                self.last_preview = Some(PreviewData::Helm(preview.clone()));
                self.preview_modal = Some(PreviewModal::new(PreviewModalKind::Preview(
                    PreviewData::Helm(preview),
                )));
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_docker_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Up => self.docker_cursor = self.docker_cursor.saturating_sub(1),
            KeyCode::Down => {
                if self.docker_cursor + 1 < self.config.docker.images.len() {
                    self.docker_cursor += 1;
                }
            }
            KeyCode::Char(' ') => {
                if let Some(selected) = self.docker_selected.get_mut(self.docker_cursor) {
                    *selected = !*selected;
                }
                self.sync_docker_selection_state();
            }
            KeyCode::Char('a') => {
                toggle_all(&mut self.docker_selected);
                self.sync_docker_selection_state();
            }
            KeyCode::Char('p') | KeyCode::Char('r') => {
                let preview = build_docker_preview(
                    &self.config,
                    &selected_docker_image_indices(&self.config, &self.docker_selected),
                );
                self.last_preview = Some(PreviewData::Docker(preview.clone()));
                self.preview_modal = Some(PreviewModal::new(PreviewModalKind::Preview(
                    PreviewData::Docker(preview),
                )));
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_jobs_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Up => self.jobs_cursor = self.jobs_cursor.saturating_sub(1),
            KeyCode::Down => {
                if self.jobs_cursor + 1 < self.jobs_list_len() {
                    self.jobs_cursor += 1;
                }
            }
            KeyCode::Char('r') => {
                self.recent_runs = recent_runs(&self.config)?;
                self.status_message = "Reloaded recent runs".to_string();
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_config_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Left => {
                self.config_focus = self.config_focus.previous();
                self.config_cursor = 0;
            }
            KeyCode::Right => {
                self.config_focus = self.config_focus.next();
                self.config_cursor = 0;
            }
            KeyCode::Up => self.config_cursor = self.config_cursor.saturating_sub(1),
            KeyCode::Down => {
                let max_index = self.current_config_section_len().saturating_sub(1);
                if self.config_cursor < max_index {
                    self.config_cursor += 1;
                }
            }
            KeyCode::Char(' ') => {
                self.toggle_current_config_item();
            }
            KeyCode::Char('a') => self.open_add_form()?,
            KeyCode::Char('d') => self.delete_current_config_item()?,
            KeyCode::Char('e') => self.open_edit_form()?,
            KeyCode::Char('s') => {
                self.config.save_with_layout(&self.config_layout)?;
                self.config_dirty = false;
                self.recent_runs = recent_runs(&self.config).unwrap_or_default();
                self.status_message = format!("Saved {}", self.config_path.display());
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_preview_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Esc => self.preview_modal = None,
            KeyCode::Enter => {
                let Some(modal) = self.preview_modal.clone() else {
                    return Ok(());
                };

                if let PreviewModalKind::ConfirmGitPreview {
                    indices, preset, ..
                } = modal.kind
                {
                    if self.preview_generation_active {
                        self.status_message =
                            "Git preview generation is already running".to_string();
                        self.status_indicator_visible = true;
                        self.preview_modal = None;
                        return Ok(());
                    }
                    if self.job_is_running() {
                        self.status_message = "A job is already running".to_string();
                        self.status_indicator_visible = true;
                        self.preview_modal = None;
                        return Ok(());
                    }

                    self.preview_generation_active = true;
                    self.status_message = format!(
                        "Preparing Git preview for {} repos in the {} window",
                        indices.len(),
                        preset.label()
                    );
                    self.status_indicator_visible = true;
                    self.preview_modal = None;
                    spawn_git_preview_generation(
                        self.config.clone(),
                        indices,
                        preset,
                        Arc::clone(&self.runner),
                        self.job_sender.clone(),
                    )
                    .await;
                    return Ok(());
                }

                if self.job_is_running() {
                    self.status_message = "A job is already running".to_string();
                    self.status_indicator_visible = true;
                    return Ok(());
                }

                let Some(preview) = modal.preview() else {
                    return Ok(());
                };
                self.status_message = "Started export job".to_string();
                self.status_indicator_visible = true;
                self.preview_modal = None;
                match preview.clone() {
                    PreviewData::Git(preview) => {
                        self.current_job = Some(CurrentJob::new(
                            JobKind::Git,
                            format!("Running Git export for {}", preview.preset.label()),
                        ));
                        spawn_git_job(
                            self.config.clone(),
                            preview,
                            Arc::clone(&self.runner),
                            self.job_sender.clone(),
                        )
                        .await;
                    }
                    PreviewData::Helm(preview) => {
                        self.current_job = Some(CurrentJob::new(
                            JobKind::Helm,
                            "Running Helm chart export".to_string(),
                        ));
                        spawn_helm_job(
                            self.config.clone(),
                            preview,
                            Arc::clone(&self.runner),
                            self.job_sender.clone(),
                        )
                        .await;
                    }
                    PreviewData::Docker(preview) => {
                        self.current_job = Some(CurrentJob::new(
                            JobKind::Docker,
                            "Running Docker image export".to_string(),
                        ));
                        spawn_docker_job(
                            self.config.clone(),
                            preview,
                            Arc::clone(&self.runner),
                            self.job_sender.clone(),
                        )
                        .await;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_form_key(&mut self, key: KeyEvent) -> Result<()> {
        let Some(form) = self.form_modal.as_mut() else {
            return Ok(());
        };

        match key.code {
            KeyCode::Esc => {
                self.form_modal = None;
            }
            KeyCode::Tab | KeyCode::Down => {
                if form.active + 1 < form.fields.len() {
                    form.active += 1;
                } else {
                    form.active = 0;
                }
            }
            KeyCode::BackTab | KeyCode::Up => {
                if form.active == 0 {
                    form.active = form.fields.len().saturating_sub(1);
                } else {
                    form.active -= 1;
                }
            }
            KeyCode::Backspace => {
                form.current_value_mut().pop();
            }
            KeyCode::Enter => {
                let form = self.form_modal.take().expect("form should exist");
                self.apply_form(form)?;
            }
            KeyCode::Char(character) => {
                if !key.modifiers.contains(KeyModifiers::CONTROL) {
                    form.current_value_mut().push(character);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_job_event(&mut self, job: JobEvent) {
        match job {
            JobEvent::Started { kind, description } => {
                self.current_job = Some(CurrentJob::new(kind, description.clone()));
                self.status_message = description;
                self.status_indicator_visible = true;
                self.jobs_cursor = 0;
            }
            JobEvent::PreviewStarted(message) => {
                self.preview_generation_active = true;
                self.status_message = message;
                self.status_indicator_visible = true;
            }
            JobEvent::PreviewReady(preview) => {
                self.preview_generation_active = false;
                self.status_message = format!(
                    "Git preview ready: {} repos will be included",
                    preview.included.len()
                );
                self.status_indicator_visible = true;
                self.last_preview = Some(PreviewData::Git(preview.clone()));
                self.preview_modal = Some(PreviewModal::new(PreviewModalKind::Preview(
                    PreviewData::Git(preview),
                )));
            }
            JobEvent::PreviewFailed(error) => {
                self.preview_generation_active = false;
                self.status_message = format!("Git preview failed: {error}");
                self.status_indicator_visible = true;
            }
            JobEvent::Log(message) => {
                if let Some(current) = self.current_job.as_mut() {
                    current.logs.push(message.clone());
                }
                self.status_message = message;
                self.status_indicator_visible = true;
            }
            JobEvent::Finished(manifest) => {
                if let Some(current) = self.current_job.as_mut() {
                    current.running = false;
                    current.manifest = Some(manifest.clone());
                }
                self.last_preview = None;
                self.status_message = manifest.summary.clone();
                self.status_indicator_visible = true;
                self.recent_runs = recent_runs(&self.config).unwrap_or_default();
                self.jobs_cursor = 0;
            }
            JobEvent::Failed(error) => {
                if let Some(current) = self.current_job.as_mut() {
                    current.running = false;
                    current.failure = Some(error.clone());
                }
                self.status_message = format!("Job failed: {error}");
                self.status_indicator_visible = true;
                self.recent_runs = recent_runs(&self.config).unwrap_or_default();
                self.jobs_cursor = 0;
            }
        }
    }

    fn draw_tabs(&self, frame: &mut Frame, area: Rect) {
        let titles = MainTab::ALL
            .iter()
            .map(|tab| Line::from(tab.title()))
            .collect::<Vec<_>>();
        let selected = self.active_tab.index();
        let tabs = Tabs::new(titles)
            .select(selected)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(theme_accent()))
                    .title("Migration Suite"),
            )
            .highlight_style(
                Style::default()
                    .fg(Color::Black)
                    .bg(theme_accent())
                    .add_modifier(Modifier::BOLD),
            );
        frame.render_widget(tabs, area);
    }

    fn draw_git_tab(&self, frame: &mut Frame, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(8),
                Constraint::Length(5),
            ])
            .split(area);
        let summary_layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
            .split(layout[0]);

        let preset_line = TimeWindowPreset::ALL
            .iter()
            .enumerate()
            .map(|(index, preset)| {
                if index == self.git_preset_index {
                    Span::styled(
                        format!("[{}] ", preset.label()),
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                    )
                } else {
                    Span::raw(format!("{} ", preset.label()))
                }
            })
            .collect::<Vec<_>>();
        frame.render_widget(
            Paragraph::new(Line::from(preset_line)).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(theme_warn()))
                    .title("Time Window"),
            ),
            summary_layout[0],
        );
        frame.render_widget(
            Paragraph::new(self.config.git.default_branches.join(", "))
                .wrap(Wrap { trim: true })
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(theme_info()))
                        .title("Default Branches"),
                ),
            summary_layout[1],
        );

        let visible_rows = visible_list_rows(layout[1]);
        let (start, end) = paged_window(self.git_cursor, self.config.git.repos.len(), visible_rows);
        let items = self
            .config
            .git
            .repos
            .iter()
            .enumerate()
            .skip(start)
            .take(end.saturating_sub(start))
            .map(|(index, repo)| {
                let style = if index == self.git_cursor {
                    Style::default()
                        .fg(theme_accent())
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                let mut spans = vec![
                    cursor_span(index == self.git_cursor),
                    Span::raw(" "),
                    selection_span(self.git_selected.get(index).copied().unwrap_or(false)),
                    Span::raw(" "),
                    Span::styled(repo.name.clone(), style),
                ];
                if let Some(branches) = repo.branches.as_ref() {
                    spans.push(Span::styled(
                        format!("  [override: {}]", branches.join(", ")),
                        Style::default().fg(theme_warn()),
                    ));
                }
                ListItem::new(Line::from(spans))
            })
            .collect::<Vec<_>>();
        frame.render_widget(
            List::new(items).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(theme_git()))
                    .title(format!(
                        "Git Repositories {}",
                        pagination_label(start, end, self.config.git.repos.len())
                    )),
            ),
            layout[1],
        );

        frame.render_widget(
            Paragraph::new(git_controls_text())
                .wrap(Wrap { trim: true })
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(theme_info()))
                        .title("Controls"),
                ),
            layout[2],
        );
    }

    fn draw_helm_tab(&self, frame: &mut Frame, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(8), Constraint::Length(5)])
            .split(area);

        let visible_rows = visible_list_rows(layout[0]);
        let (start, end) = paged_window(
            self.helm_cursor,
            self.config.helm.charts.len(),
            visible_rows,
        );
        let visible_charts = self
            .config
            .helm
            .charts
            .iter()
            .enumerate()
            .skip(start)
            .take(end.saturating_sub(start))
            .collect::<Vec<_>>();
        let chart_name_width = visible_charts
            .iter()
            .map(|(_, chart)| chart.name.len())
            .max()
            .unwrap_or(0);
        let items = self
            .config
            .helm
            .charts
            .iter()
            .enumerate()
            .skip(start)
            .take(end.saturating_sub(start))
            .map(|(index, chart)| {
                let style = if index == self.helm_cursor {
                    Style::default()
                        .fg(theme_accent())
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                let name = format!("{:<width$}", chart.name, width = chart_name_width);
                ListItem::new(Line::from(vec![
                    cursor_span(index == self.helm_cursor),
                    Span::raw(" "),
                    selection_span(self.helm_selected.get(index).copied().unwrap_or(false)),
                    Span::raw(" "),
                    Span::styled(name, style),
                    Span::styled(
                        format!("  {}", chart.version),
                        Style::default().fg(theme_info()),
                    ),
                ]))
            })
            .collect::<Vec<_>>();
        frame.render_widget(
            List::new(items).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(theme_helm()))
                    .title(format!(
                        "Helm Charts {}",
                        pagination_label(start, end, self.config.helm.charts.len())
                    )),
            ),
            layout[0],
        );

        frame.render_widget(
            Paragraph::new(artifact_controls_text("charts"))
                .wrap(Wrap { trim: true })
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(theme_info()))
                        .title("Controls"),
                ),
            layout[1],
        );
    }

    fn draw_docker_tab(&self, frame: &mut Frame, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(8), Constraint::Length(5)])
            .split(area);

        let visible_rows = visible_list_rows(layout[0]);
        let (start, end) = paged_window(
            self.docker_cursor,
            self.config.docker.images.len(),
            visible_rows,
        );
        let visible_images = self
            .config
            .docker
            .images
            .iter()
            .enumerate()
            .skip(start)
            .take(end.saturating_sub(start))
            .collect::<Vec<_>>();
        let image_name_width = visible_images
            .iter()
            .map(|(_, image)| image.name.len())
            .max()
            .unwrap_or(0);
        let items = self
            .config
            .docker
            .images
            .iter()
            .enumerate()
            .skip(start)
            .take(end.saturating_sub(start))
            .map(|(index, image)| {
                let style = if index == self.docker_cursor {
                    Style::default()
                        .fg(theme_accent())
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                let name = format!("{:<width$}", image.name, width = image_name_width);
                ListItem::new(Line::from(vec![
                    cursor_span(index == self.docker_cursor),
                    Span::raw(" "),
                    selection_span(self.docker_selected.get(index).copied().unwrap_or(false)),
                    Span::raw(" "),
                    Span::styled(name, style),
                    Span::styled(
                        format!("  {}", image.tag),
                        Style::default().fg(theme_info()),
                    ),
                ]))
            })
            .collect::<Vec<_>>();
        frame.render_widget(
            List::new(items).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(theme_docker()))
                    .title(format!(
                        "Docker Images {}",
                        pagination_label(start, end, self.config.docker.images.len())
                    )),
            ),
            layout[0],
        );

        frame.render_widget(
            Paragraph::new(artifact_controls_text("images"))
                .wrap(Wrap { trim: true })
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(theme_info()))
                        .title("Controls"),
                ),
            layout[1],
        );
    }

    fn draw_jobs_tab(&self, frame: &mut Frame, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
            .split(area);

        let mut items = Vec::new();
        let mut list_index = 0;

        if let Some(job) = self.current_job.as_ref().filter(|job| job.running) {
            let row_style = if list_index == self.jobs_cursor {
                Style::default()
                    .fg(theme_accent())
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            items.push(ListItem::new(Line::from(vec![
                cursor_span(list_index == self.jobs_cursor),
                Span::raw(" "),
                status_badge_span("RUN"),
                Span::raw(" "),
                Span::styled(
                    format!("[{}]", job.kind.as_str()),
                    Style::default().fg(theme_info()),
                ),
                Span::raw(" "),
                Span::styled(job.description.clone(), row_style),
            ])));
            list_index += 1;
        }

        items.extend(
            self.recent_runs
                .iter()
                .enumerate()
                .map(|(index, manifest)| {
                    let item_index = list_index + index;
                    let row_style = if item_index == self.jobs_cursor {
                        Style::default()
                            .fg(theme_accent())
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    };
                    ListItem::new(Line::from(vec![
                        cursor_span(item_index == self.jobs_cursor),
                        Span::raw(" "),
                        status_badge_span(manifest_run_label(manifest.status.clone())),
                        Span::raw(" "),
                        Span::styled(
                            format!("[{}]", manifest.kind.as_str()),
                            Style::default().fg(theme_info()),
                        ),
                        Span::raw(" "),
                        Span::styled(manifest.summary.clone(), row_style),
                    ]))
                }),
        );

        frame.render_widget(
            List::new(items).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(theme_info()))
                    .title("Recent Runs"),
            ),
            layout[0],
        );

        let details = self.selected_job_details();
        frame.render_widget(
            Paragraph::new(details).wrap(Wrap { trim: true }).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(theme_info()))
                    .title("Job Details"),
            ),
            layout[1],
        );
    }

    fn draw_config_tab(&self, frame: &mut Frame, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(10)])
            .split(area);

        let titles = ConfigSection::ALL
            .iter()
            .map(|section| Line::from(section.title()))
            .collect::<Vec<_>>();
        let tabs = Tabs::new(titles)
            .select(self.config_focus.index())
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(theme_muted()))
                    .title("Config"),
            )
            .highlight_style(
                Style::default()
                    .fg(Color::Black)
                    .bg(theme_warn())
                    .add_modifier(Modifier::BOLD),
            );
        frame.render_widget(tabs, layout[0]);

        let body = match self.config_focus {
            ConfigSection::Output => format!(
                "Output directory: {}\nRecent run limit: {}\nSplit large transfers: {}\nMax transfer size (MB): {}\n\nPress `e` to edit and `s` to save.",
                self.config.output.base_dir.display(),
                self.config.output.recent_run_limit,
                if self.config.output.split_large_transfers {
                    "enabled"
                } else {
                    "disabled"
                },
                self.config.output.max_transfer_size_mb
            ),
            ConfigSection::GitDefaults => format!(
                "Default branches: {}\n\nPress `e` to edit and `s` to save.",
                self.config.git.default_branches.join(", ")
            ),
            ConfigSection::GitRepos => render_git_repo_config_list(
                &self.config.git.repos,
                self.config_cursor,
                self.config.git.default_branches.as_slice(),
            ),
            ConfigSection::HelmCharts => {
                render_helm_chart_config_list(&self.config.helm.charts, self.config_cursor)
            }
            ConfigSection::DockerImages => {
                render_docker_image_config_list(&self.config.docker.images, self.config_cursor)
            }
        };

        let title = if self.config_dirty {
            "Config (unsaved changes)"
        } else {
            "Config Details"
        };
        frame.render_widget(
            Paragraph::new(body).wrap(Wrap { trim: true }).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(theme_muted()))
                    .title(title),
            ),
            layout[1],
        );
    }

    fn draw_footer(&self, frame: &mut Frame, area: Rect) {
        let hints = match self.active_tab {
            MainTab::Git => {
                "Tab switch  Up/Down move  Space toggle  a toggle all  Left/Right window  p preview  Enter run in modal  q quit"
            }
            MainTab::Helm | MainTab::Docker => {
                "Tab switch  Up/Down move  Space toggle  a toggle all  p preview  Enter run in modal  q quit"
            }
            MainTab::Jobs => "Tab switch  Up/Down move  r reload recent manifests  q quit",
            MainTab::Config => {
                "Tab switch  Left/Right section  Up/Down move  e edit  a add  d delete  Space toggle  s save  q quit"
            }
        };
        let footer = Paragraph::new(vec![
            Line::from(self.footer_status_spans()),
            Line::from(Span::styled(hints, Style::default().fg(theme_muted()))),
        ])
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true })
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(self.footer_border_color()))
                .title("Status"),
        );
        frame.render_widget(footer, area);
    }

    fn draw_preview_modal(&self, frame: &mut Frame, modal: &PreviewModal) {
        let area = centered_rect(80, 70, frame.area());
        frame.render_widget(Clear, area);
        let body = modal.summary();
        let (title, color) = modal.title_and_color();
        frame.render_widget(
            Paragraph::new(body).wrap(Wrap { trim: true }).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(color))
                    .title(title),
            ),
            area,
        );
    }

    fn draw_form_modal(&self, frame: &mut Frame, form: &FormModal) {
        let area = centered_rect(70, 60, frame.area());
        frame.render_widget(Clear, area);
        let lines = form
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let prefix = if index == form.active { "> " } else { "  " };
                Line::from(format!("{prefix}{}: {}", field.label, field.value))
            })
            .collect::<Vec<_>>();
        frame.render_widget(
            Paragraph::new(lines).wrap(Wrap { trim: true }).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(theme_warn()))
                    .title(format!("{} (Enter to save, Esc to cancel)", form.title)),
            ),
            area,
        );
    }

    fn job_is_running(&self) -> bool {
        self.current_job
            .as_ref()
            .map(|job| job.running)
            .unwrap_or(false)
    }

    fn jobs_list_len(&self) -> usize {
        self.recent_runs.len() + usize::from(self.job_is_running())
    }

    fn selected_job_details(&self) -> String {
        if self.job_is_running() && self.jobs_cursor == 0 {
            return self
                .current_job
                .as_ref()
                .map(render_current_job)
                .unwrap_or_else(|| "No active or recent jobs yet.".to_string());
        }

        let manifest_index = self
            .jobs_cursor
            .saturating_sub(usize::from(self.job_is_running()));
        if let Some(manifest) = self.recent_runs.get(manifest_index) {
            render_manifest(manifest)
        } else if let Some(job) = self.current_job.as_ref() {
            render_current_job(job)
        } else {
            "No active or recent jobs yet.".to_string()
        }
    }

    fn request_quit(&mut self) -> Result<()> {
        if self.config_dirty {
            self.save_config_to_disk()?;
        }
        self.running = false;
        Ok(())
    }

    fn set_active_tab(&mut self, tab: MainTab) {
        self.active_tab = tab;
        if matches!(self.active_tab, MainTab::Jobs) {
            self.status_indicator_visible = false;
        }
    }

    fn footer_status_spans(&self) -> Vec<Span<'static>> {
        if self.preview_generation_active {
            return vec![
                status_badge_span("PREVIEW"),
                Span::raw(" "),
                Span::styled(
                    self.status_message.clone(),
                    Style::default().fg(Color::White),
                ),
            ];
        }
        if self.status_indicator_visible {
            if let Some(job) = self.current_job.as_ref() {
                return vec![
                    status_badge_span(job_status_label(job)),
                    Span::raw(" "),
                    Span::styled(compact_job_status(job), Style::default().fg(Color::White)),
                ];
            }
        }
        vec![
            Span::styled(
                "STATUS",
                Style::default()
                    .fg(theme_info())
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" "),
            Span::styled(
                self.status_message.clone(),
                Style::default().fg(Color::White),
            ),
        ]
    }

    fn footer_border_color(&self) -> Color {
        if self.preview_generation_active {
            return theme_warn();
        }
        if self.status_indicator_visible {
            if let Some(job) = self.current_job.as_ref() {
                return status_label_color(job_status_label(job));
            }
        }
        theme_muted()
    }

    fn current_config_section_len(&self) -> usize {
        match self.config_focus {
            ConfigSection::Output | ConfigSection::GitDefaults => 1,
            ConfigSection::GitRepos => self.config.git.repos.len(),
            ConfigSection::HelmCharts => self.config.helm.charts.len(),
            ConfigSection::DockerImages => self.config.docker.images.len(),
        }
    }

    fn sync_git_selection_state(&mut self) {
        for (repo, selected) in self
            .config
            .git
            .repos
            .iter_mut()
            .zip(self.git_selected.iter().copied())
        {
            repo.enabled = selected;
        }
        self.config_dirty = true;
        self.status_message = format!(
            "Git selections updated: {} enabled",
            self.git_selected
                .iter()
                .filter(|selected| **selected)
                .count()
        );
    }

    fn open_git_preview_confirmation(&mut self) -> Result<()> {
        if self.preview_generation_active {
            self.status_message = "Git preview generation is already running".to_string();
            self.status_indicator_visible = true;
            return Ok(());
        }

        let indices = selected_git_repo_indices(&self.config, &self.git_selected);
        if indices.is_empty() {
            return Err(eyre!(
                "Select at least one Git repository before previewing"
            ));
        }

        let preset = TimeWindowPreset::ALL[self.git_preset_index];
        self.preview_modal = Some(PreviewModal::new(PreviewModalKind::ConfirmGitPreview {
            repo_count: indices.len(),
            indices,
            preset,
        }));
        Ok(())
    }

    fn sync_helm_selection_state(&mut self) {
        for (chart, selected) in self
            .config
            .helm
            .charts
            .iter_mut()
            .zip(self.helm_selected.iter().copied())
        {
            chart.enabled = selected;
        }
        self.config_dirty = true;
        self.status_message = format!(
            "Helm selections updated: {} enabled",
            self.helm_selected
                .iter()
                .filter(|selected| **selected)
                .count()
        );
    }

    fn sync_docker_selection_state(&mut self) {
        for (image, selected) in self
            .config
            .docker
            .images
            .iter_mut()
            .zip(self.docker_selected.iter().copied())
        {
            image.enabled = selected;
        }
        self.config_dirty = true;
        self.status_message = format!(
            "Docker selections updated: {} enabled",
            self.docker_selected
                .iter()
                .filter(|selected| **selected)
                .count()
        );
    }

    fn save_config_to_disk(&mut self) -> Result<()> {
        self.config.save_with_layout(&self.config_layout)?;
        self.config_dirty = false;
        self.recent_runs = recent_runs(&self.config).unwrap_or_default();
        self.status_message = format!("Saved {}", self.config_path.display());
        Ok(())
    }

    fn toggle_current_config_item(&mut self) {
        match self.config_focus {
            ConfigSection::GitRepos => {
                if let Some(item) = self.config.git.repos.get_mut(self.config_cursor) {
                    item.enabled = !item.enabled;
                    self.config_dirty = true;
                    if let Some(selected) = self.git_selected.get_mut(self.config_cursor) {
                        *selected = item.enabled;
                    }
                }
            }
            ConfigSection::HelmCharts => {
                if let Some(item) = self.config.helm.charts.get_mut(self.config_cursor) {
                    item.enabled = !item.enabled;
                    self.config_dirty = true;
                    if let Some(selected) = self.helm_selected.get_mut(self.config_cursor) {
                        *selected = item.enabled;
                    }
                }
            }
            ConfigSection::DockerImages => {
                if let Some(item) = self.config.docker.images.get_mut(self.config_cursor) {
                    item.enabled = !item.enabled;
                    self.config_dirty = true;
                    if let Some(selected) = self.docker_selected.get_mut(self.config_cursor) {
                        *selected = item.enabled;
                    }
                }
            }
            ConfigSection::Output | ConfigSection::GitDefaults => {}
        }
    }

    fn open_add_form(&mut self) -> Result<()> {
        self.form_modal = Some(match self.config_focus {
            ConfigSection::GitRepos => FormModal::new(
                "Add Git Repo",
                FormKind::AddGitRepo,
                vec![
                    ("Name", String::new()),
                    ("Path", String::new()),
                    ("Remote", "origin".to_string()),
                    ("Branches CSV", String::new()),
                ],
            ),
            ConfigSection::HelmCharts => FormModal::new(
                "Add Helm Chart",
                FormKind::AddHelmChart,
                vec![
                    ("Name", String::new()),
                    ("Reference", String::new()),
                    ("Version", String::new()),
                ],
            ),
            ConfigSection::DockerImages => FormModal::new(
                "Add Docker Image",
                FormKind::AddDockerImage,
                vec![
                    ("Name", String::new()),
                    ("Repository", String::new()),
                    ("Tag", String::new()),
                ],
            ),
            ConfigSection::Output | ConfigSection::GitDefaults => {
                return Err(eyre!("add is not supported in this config section"));
            }
        });
        Ok(())
    }

    fn open_edit_form(&mut self) -> Result<()> {
        self.form_modal = Some(match self.config_focus {
            ConfigSection::Output => FormModal::new(
                "Edit Output Settings",
                FormKind::EditOutput,
                vec![
                    (
                        "Base Directory",
                        self.config.output.base_dir.display().to_string(),
                    ),
                    (
                        "Recent Run Limit",
                        self.config.output.recent_run_limit.to_string(),
                    ),
                    (
                        "Split Large Transfers",
                        self.config.output.split_large_transfers.to_string(),
                    ),
                    (
                        "Max Transfer Size MB",
                        self.config.output.max_transfer_size_mb.to_string(),
                    ),
                ],
            ),
            ConfigSection::GitDefaults => FormModal::new(
                "Edit Default Branches",
                FormKind::EditGitDefaults,
                vec![(
                    "Branches CSV",
                    branches_to_csv(&self.config.git.default_branches),
                )],
            ),
            ConfigSection::GitRepos => {
                let repo = self
                    .config
                    .git
                    .repos
                    .get(self.config_cursor)
                    .ok_or_else(|| eyre!("no git repo selected"))?;
                FormModal::new(
                    "Edit Git Repo",
                    FormKind::EditGitRepo(self.config_cursor),
                    vec![
                        ("Name", repo.name.clone()),
                        ("Path", repo.path.display().to_string()),
                        ("Remote", repo.remote.clone().unwrap_or_default()),
                        (
                            "Branches CSV",
                            repo.branches
                                .as_ref()
                                .map(|branches| branches.join(","))
                                .unwrap_or_default(),
                        ),
                    ],
                )
            }
            ConfigSection::HelmCharts => {
                let chart = self
                    .config
                    .helm
                    .charts
                    .get(self.config_cursor)
                    .ok_or_else(|| eyre!("no helm chart selected"))?;
                FormModal::new(
                    "Edit Helm Chart",
                    FormKind::EditHelmChart(self.config_cursor),
                    vec![
                        ("Name", chart.name.clone()),
                        ("Reference", chart.reference.clone()),
                        ("Version", chart.version.clone()),
                    ],
                )
            }
            ConfigSection::DockerImages => {
                let image = self
                    .config
                    .docker
                    .images
                    .get(self.config_cursor)
                    .ok_or_else(|| eyre!("no docker image selected"))?;
                FormModal::new(
                    "Edit Docker Image",
                    FormKind::EditDockerImage(self.config_cursor),
                    vec![
                        ("Name", image.name.clone()),
                        ("Repository", image.repository.clone()),
                        ("Tag", image.tag.clone()),
                    ],
                )
            }
        });
        Ok(())
    }

    fn delete_current_config_item(&mut self) -> Result<()> {
        match self.config_focus {
            ConfigSection::GitRepos => {
                if self.config_cursor < self.config.git.repos.len() {
                    self.config.git.repos.remove(self.config_cursor);
                    if self.config_cursor < self.git_selected.len() {
                        self.git_selected.remove(self.config_cursor);
                    }
                    self.config_dirty = true;
                }
            }
            ConfigSection::HelmCharts => {
                if self.config_cursor < self.config.helm.charts.len() {
                    self.config.helm.charts.remove(self.config_cursor);
                    if self.config_cursor < self.helm_selected.len() {
                        self.helm_selected.remove(self.config_cursor);
                    }
                    self.config_dirty = true;
                }
            }
            ConfigSection::DockerImages => {
                if self.config_cursor < self.config.docker.images.len() {
                    self.config.docker.images.remove(self.config_cursor);
                    if self.config_cursor < self.docker_selected.len() {
                        self.docker_selected.remove(self.config_cursor);
                    }
                    self.config_dirty = true;
                }
            }
            ConfigSection::Output | ConfigSection::GitDefaults => {
                return Err(eyre!("delete is not supported in this config section"));
            }
        }
        self.config_cursor = self
            .config_cursor
            .min(self.current_config_section_len().saturating_sub(1));
        Ok(())
    }

    fn apply_form(&mut self, form: FormModal) -> Result<()> {
        let values = form.values();
        match form.kind {
            FormKind::EditOutput => {
                self.config.output.base_dir = PathBuf::from(values[0].clone());
                self.config.output.recent_run_limit = values[1].parse::<usize>()?;
                self.config.output.split_large_transfers = parse_bool_flag(&values[2])?;
                self.config.output.max_transfer_size_mb = values[3].parse::<u64>()?;
            }
            FormKind::EditGitDefaults => {
                self.config.git.default_branches = csv_to_branches(&values[0]);
            }
            FormKind::AddGitRepo => {
                self.config.git.repos.push(GitRepoConfig {
                    name: values[0].clone(),
                    path: PathBuf::from(values[1].clone()),
                    remote: optional_string(&values[2]),
                    branches: optional_branches(&values[3]),
                    enabled: true,
                });
                self.git_selected.push(true);
            }
            FormKind::EditGitRepo(index) => {
                let repo = self
                    .config
                    .git
                    .repos
                    .get_mut(index)
                    .ok_or_else(|| eyre!("invalid git repo index"))?;
                repo.name = values[0].clone();
                repo.path = PathBuf::from(values[1].clone());
                repo.remote = optional_string(&values[2]);
                repo.branches = optional_branches(&values[3]);
            }
            FormKind::AddHelmChart => {
                self.config.helm.charts.push(HelmChartConfig {
                    name: values[0].clone(),
                    reference: values[1].clone(),
                    version: values[2].clone(),
                    enabled: true,
                });
                self.helm_selected.push(true);
            }
            FormKind::EditHelmChart(index) => {
                let chart = self
                    .config
                    .helm
                    .charts
                    .get_mut(index)
                    .ok_or_else(|| eyre!("invalid helm chart index"))?;
                chart.name = values[0].clone();
                chart.reference = values[1].clone();
                chart.version = values[2].clone();
            }
            FormKind::AddDockerImage => {
                self.config.docker.images.push(DockerImageConfig {
                    name: values[0].clone(),
                    repository: values[1].clone(),
                    tag: values[2].clone(),
                    enabled: true,
                });
                self.docker_selected.push(true);
            }
            FormKind::EditDockerImage(index) => {
                let image = self
                    .config
                    .docker
                    .images
                    .get_mut(index)
                    .ok_or_else(|| eyre!("invalid docker image index"))?;
                image.name = values[0].clone();
                image.repository = values[1].clone();
                image.tag = values[2].clone();
            }
        }
        self.config.validate()?;
        self.config_dirty = true;
        self.status_message = "Updated configuration in memory".to_string();
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MainTab {
    Git,
    Helm,
    Docker,
    Jobs,
    Config,
}

impl MainTab {
    const ALL: [Self; 5] = [
        Self::Git,
        Self::Helm,
        Self::Docker,
        Self::Jobs,
        Self::Config,
    ];

    fn title(&self) -> &'static str {
        match self {
            Self::Git => "Git",
            Self::Helm => "Helm",
            Self::Docker => "Docker",
            Self::Jobs => "Jobs",
            Self::Config => "Config",
        }
    }

    fn index(&self) -> usize {
        match self {
            Self::Git => 0,
            Self::Helm => 1,
            Self::Docker => 2,
            Self::Jobs => 3,
            Self::Config => 4,
        }
    }

    fn next(&self) -> Self {
        Self::ALL[(self.index() + 1) % Self::ALL.len()]
    }

    fn previous(&self) -> Self {
        if self.index() == 0 {
            Self::ALL[Self::ALL.len() - 1]
        } else {
            Self::ALL[self.index() - 1]
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConfigSection {
    Output,
    GitDefaults,
    GitRepos,
    HelmCharts,
    DockerImages,
}

impl ConfigSection {
    const ALL: [Self; 5] = [
        Self::Output,
        Self::GitDefaults,
        Self::GitRepos,
        Self::HelmCharts,
        Self::DockerImages,
    ];

    fn title(&self) -> &'static str {
        match self {
            Self::Output => "Output",
            Self::GitDefaults => "Git Defaults",
            Self::GitRepos => "Git Repos",
            Self::HelmCharts => "Helm Charts",
            Self::DockerImages => "Docker Images",
        }
    }

    fn index(&self) -> usize {
        match self {
            Self::Output => 0,
            Self::GitDefaults => 1,
            Self::GitRepos => 2,
            Self::HelmCharts => 3,
            Self::DockerImages => 4,
        }
    }

    fn next(&self) -> Self {
        Self::ALL[(self.index() + 1) % Self::ALL.len()]
    }

    fn previous(&self) -> Self {
        if self.index() == 0 {
            Self::ALL[Self::ALL.len() - 1]
        } else {
            Self::ALL[self.index() - 1]
        }
    }
}

#[derive(Clone, Debug)]
struct PreviewModal {
    kind: PreviewModalKind,
}

impl PreviewModal {
    fn new(kind: PreviewModalKind) -> Self {
        Self { kind }
    }

    fn preview(&self) -> Option<PreviewData> {
        match &self.kind {
            PreviewModalKind::Preview(preview) => Some(preview.clone()),
            PreviewModalKind::ConfirmGitPreview { .. } => None,
        }
    }

    fn title_and_color(&self) -> (String, Color) {
        match &self.kind {
            PreviewModalKind::ConfirmGitPreview { .. } => {
                ("Prepare Git Preview".to_string(), theme_warn())
            }
            PreviewModalKind::Preview(preview) => (
                format!("{} Preview", preview.title()),
                preview_kind_color(preview),
            ),
        }
    }

    fn summary(&self) -> String {
        match &self.kind {
            PreviewModalKind::ConfirmGitPreview {
                repo_count, preset, ..
            } => format!(
                "This Git preview may take a little while while we fetch updates and determine which repositories should be bundled.\n\nSelected repos: {repo_count}\nTime window: {}\n\nPress Enter to start preparing the preview or Esc to cancel.",
                preset.label()
            ),
            PreviewModalKind::Preview(PreviewData::Git(preview)) => format!(
                "{}\n\nPress Enter to start the Git export or Esc to cancel.",
                render_git_preview_modal(preview)
            ),
            PreviewModalKind::Preview(PreviewData::Helm(preview)) => format!(
                "{}\n\nPress Enter to start the Helm export or Esc to cancel.",
                render_helm_preview_modal(preview)
            ),
            PreviewModalKind::Preview(PreviewData::Docker(preview)) => format!(
                "{}\n\nPress Enter to start the Docker export or Esc to cancel.",
                render_docker_preview_modal(preview)
            ),
        }
    }
}

#[derive(Clone, Debug)]
enum PreviewModalKind {
    ConfirmGitPreview {
        repo_count: usize,
        indices: Vec<usize>,
        preset: TimeWindowPreset,
    },
    Preview(PreviewData),
}

#[derive(Clone, Debug)]
struct FormModal {
    title: String,
    kind: FormKind,
    fields: Vec<FormField>,
    active: usize,
}

impl FormModal {
    fn new(title: &str, kind: FormKind, fields: Vec<(&str, String)>) -> Self {
        Self {
            title: title.to_string(),
            kind,
            fields: fields
                .into_iter()
                .map(|(label, value)| FormField {
                    label: label.to_string(),
                    value,
                })
                .collect(),
            active: 0,
        }
    }

    fn current_value_mut(&mut self) -> &mut String {
        &mut self.fields[self.active].value
    }

    fn values(&self) -> Vec<String> {
        self.fields
            .iter()
            .map(|field| field.value.trim().to_string())
            .collect()
    }
}

#[derive(Clone, Debug)]
struct FormField {
    label: String,
    value: String,
}

#[derive(Clone, Debug)]
enum FormKind {
    EditOutput,
    EditGitDefaults,
    AddGitRepo,
    EditGitRepo(usize),
    AddHelmChart,
    EditHelmChart(usize),
    AddDockerImage,
    EditDockerImage(usize),
}

#[derive(Clone, Debug)]
struct CurrentJob {
    kind: JobKind,
    description: String,
    running: bool,
    logs: Vec<String>,
    manifest: Option<RunManifest>,
    failure: Option<String>,
}

impl CurrentJob {
    fn new(kind: JobKind, description: String) -> Self {
        Self {
            kind,
            description,
            running: true,
            logs: Vec::new(),
            manifest: None,
            failure: None,
        }
    }
}

fn git_controls_text() -> String {
    "Press `space` to toggle the selected repo.\nPress `a` to toggle all repos.\nPress `left` and `right` to change the time window.\nPress `p` to preview, then `Enter` in the modal to run.".to_string()
}

fn artifact_controls_text(noun: &str) -> String {
    format!(
        "Press `space` to toggle the selected {noun}.\nPress `a` to toggle all {noun}.\nPress `p` to preview, then `Enter` in the modal to run."
    )
}

fn render_git_preview_modal(preview: &GitPreview) -> String {
    let mut lines = vec![
        format!("Window: {}", preview.preset.label()),
        format!("Will export: {} repos", preview.included.len()),
        format!("Will skip: {} repos", preview.skipped.len()),
    ];

    let tagged = preview
        .included
        .iter()
        .filter(|repo| !repo.tags_in_window.is_empty())
        .count();
    if tagged > 0 {
        lines.push(format!("Repos with new tags: {}", tagged));
    }

    let overrides = preview
        .included
        .iter()
        .filter(|repo| !repo.changed_branches.is_empty())
        .collect::<Vec<_>>();
    if !overrides.is_empty() {
        lines.push(String::new());
        lines.push("Changed branches:".to_string());
        for repo in overrides.iter().take(5) {
            lines.push(format!(
                "- {}: {}",
                repo.name,
                repo.changed_branches.join(", ")
            ));
        }
        if overrides.len() > 5 {
            lines.push(format!("- ...and {} more repos", overrides.len() - 5));
        }
    }

    lines.join("\n")
}

fn render_helm_preview_modal(preview: &HelmPreview) -> String {
    let mut lines = vec![
        format!("Charts selected: {}", preview.charts.len()),
        format!("Output: {}", preview.output_name),
        "Action: pull the selected chart versions and package one transfer payload.".to_string(),
    ];

    if !preview.charts.is_empty() {
        lines.push(String::new());
        lines.push("Versions:".to_string());
        for chart in preview.charts.iter().take(6) {
            lines.push(format!("- {} {}", chart.name, chart.version));
        }
        if preview.charts.len() > 6 {
            lines.push(format!("- ...and {} more charts", preview.charts.len() - 6));
        }
    }

    lines.join("\n")
}

fn render_docker_preview_modal(preview: &DockerPreview) -> String {
    let mut lines = vec![
        format!("Images selected: {}", preview.images.len()),
        "Action: export one transfer file per selected image.".to_string(),
    ];

    if !preview.images.is_empty() {
        let image_name_width = preview
            .images
            .iter()
            .map(|image| image.name.len())
            .max()
            .unwrap_or(0);
        lines.push(String::new());
        lines.push("Outputs:".to_string());
        for image in preview.images.iter().take(6) {
            lines.push(format!(
                "- {:<width$}  {}",
                image.name,
                image.output_name,
                width = image_name_width
            ));
        }
        if preview.images.len() > 6 {
            lines.push(format!("- ...and {} more files", preview.images.len() - 6));
        }
    }

    lines.join("\n")
}

fn render_manifest(manifest: &RunManifest) -> String {
    let mut lines = vec![
        format!("Kind: {}", manifest.kind.as_str()),
        format!(
            "Status: {}",
            match manifest.status {
                RunStatus::Success => "success",
                RunStatus::Failed => "failed",
            }
        ),
        format!("Summary: {}", manifest.summary),
        format!("Started: {}", manifest.started_at),
        format!("Finished: {}", manifest.finished_at),
        format!("Output dir: {}", manifest.output_dir.display()),
    ];

    lines.push(String::new());
    lines.push("Outputs:".to_string());
    if manifest.outputs.is_empty() {
        lines.push("- none".to_string());
    } else {
        for output in &manifest.outputs {
            lines.push(format!(
                "- {} => {} (sha256: {})",
                output.label,
                output.path.display(),
                output.sha256
            ));
        }
    }

    if !manifest.notes.is_empty() {
        lines.push(String::new());
        lines.push("Notes:".to_string());
        for note in &manifest.notes {
            lines.push(format!("- {note}"));
        }
    }

    if !manifest.logs.is_empty() {
        lines.push(String::new());
        lines.push("Recent logs:".to_string());
        for entry in manifest.logs.iter().rev().take(12).rev() {
            lines.push(format!("- {}", entry.message));
        }
    }
    lines.join("\n")
}

fn render_current_job(job: &CurrentJob) -> String {
    let mut lines = vec![
        format!("Active kind: {}", job.kind.as_str()),
        format!("Description: {}", job.description),
        format!("Running: {}", if job.running { "yes" } else { "no" }),
        String::new(),
        "Logs:".to_string(),
    ];
    for line in job.logs.iter().rev().take(12).rev() {
        lines.push(format!("- {line}"));
    }
    if let Some(failure) = job.failure.as_ref() {
        lines.push(String::new());
        lines.push(format!("Failure: {failure}"));
    }
    if let Some(manifest) = job.manifest.as_ref() {
        lines.push(String::new());
        lines.push(format!("Completed: {}", manifest.summary));
    }
    lines.join("\n")
}

fn compact_job_status(job: &CurrentJob) -> String {
    if job.running {
        let detail = job
            .logs
            .last()
            .cloned()
            .unwrap_or_else(|| job.description.clone());
        format!("[running] {} | {}", job.kind.as_str(), detail)
    } else if let Some(failure) = job.failure.as_ref() {
        format!("[failed] {} | {}", job.kind.as_str(), failure)
    } else if let Some(manifest) = job.manifest.as_ref() {
        format!("[complete] {} | {}", job.kind.as_str(), manifest.summary)
    } else {
        format!("[idle] {}", job.description)
    }
}

fn cursor_span(selected: bool) -> Span<'static> {
    if selected {
        Span::styled(
            ">",
            Style::default()
                .fg(theme_accent())
                .add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled(" ", Style::default().fg(theme_muted()))
    }
}

fn selection_span(selected: bool) -> Span<'static> {
    if selected {
        Span::styled(
            "[x]",
            Style::default()
                .fg(theme_success())
                .add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled("[ ]", Style::default().fg(theme_muted()))
    }
}

fn status_badge_span(label: &'static str) -> Span<'static> {
    Span::styled(
        format!(" {} ", label),
        Style::default()
            .fg(Color::Black)
            .bg(status_label_color(label))
            .add_modifier(Modifier::BOLD),
    )
}

fn preview_kind_color(preview: &PreviewData) -> Color {
    match preview {
        PreviewData::Git(_) => theme_git(),
        PreviewData::Helm(_) => theme_helm(),
        PreviewData::Docker(_) => theme_docker(),
    }
}

fn manifest_run_label(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Success => "OK",
        RunStatus::Failed => "FAIL",
    }
}

fn job_status_label(job: &CurrentJob) -> &'static str {
    if job.running {
        "RUN"
    } else if job.failure.is_some() {
        "FAIL"
    } else if job.manifest.is_some() {
        "DONE"
    } else {
        "IDLE"
    }
}

fn status_label_color(label: &str) -> Color {
    match label {
        "PREVIEW" | "RUN" => theme_warn(),
        "DONE" | "OK" => theme_success(),
        "FAIL" => theme_error(),
        "IDLE" => theme_info(),
        _ => theme_info(),
    }
}

fn theme_accent() -> Color {
    Color::Cyan
}

fn theme_info() -> Color {
    Color::LightBlue
}

fn theme_success() -> Color {
    Color::LightGreen
}

fn theme_warn() -> Color {
    Color::Yellow
}

fn theme_error() -> Color {
    Color::LightRed
}

fn theme_muted() -> Color {
    Color::DarkGray
}

fn theme_git() -> Color {
    Color::LightCyan
}

fn theme_helm() -> Color {
    Color::LightBlue
}

fn theme_docker() -> Color {
    Color::LightMagenta
}

fn render_git_repo_config_list(
    repos: &[GitRepoConfig],
    cursor: usize,
    defaults: &[String],
) -> String {
    let mut lines = vec![
        "Press `a` to add, `e` to edit, `d` to delete, `space` to toggle.".to_string(),
        String::new(),
    ];
    for (index, repo) in repos.iter().enumerate() {
        let prefix = if index == cursor { "> " } else { "  " };
        let branches = repo.branches(defaults).join(", ");
        lines.push(format!(
            "{prefix}{} [{}] path={} branches={}",
            repo.name,
            if repo.enabled { "enabled" } else { "disabled" },
            repo.path.display(),
            branches
        ));
    }
    lines.join("\n")
}

fn render_helm_chart_config_list(charts: &[HelmChartConfig], cursor: usize) -> String {
    let mut lines = vec![
        "Press `a` to add, `e` to edit, `d` to delete, `space` to toggle.".to_string(),
        String::new(),
    ];
    for (index, chart) in charts.iter().enumerate() {
        let prefix = if index == cursor { "> " } else { "  " };
        lines.push(format!(
            "{prefix}{} [{}] {} {}",
            chart.name,
            if chart.enabled { "enabled" } else { "disabled" },
            chart.reference,
            chart.version
        ));
    }
    lines.join("\n")
}

fn render_docker_image_config_list(images: &[DockerImageConfig], cursor: usize) -> String {
    let mut lines = vec![
        "Press `a` to add, `e` to edit, `d` to delete, `space` to toggle.".to_string(),
        String::new(),
    ];
    for (index, image) in images.iter().enumerate() {
        let prefix = if index == cursor { "> " } else { "  " };
        lines.push(format!(
            "{prefix}{} [{}] {}:{}",
            image.name,
            if image.enabled { "enabled" } else { "disabled" },
            image.repository,
            image.tag
        ));
    }
    lines.join("\n")
}

fn optional_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn optional_branches(value: &str) -> Option<Vec<String>> {
    let branches = csv_to_branches(value);
    if branches.is_empty() {
        None
    } else {
        Some(branches)
    }
}

fn parse_bool_flag(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "yes" | "y" | "1" | "on" | "enabled" => Ok(true),
        "false" | "no" | "n" | "0" | "off" | "disabled" => Ok(false),
        _ => Err(eyre!("expected a boolean value like true/false or yes/no")),
    }
}

fn centered_rect(horizontal: u16, vertical: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - vertical) / 2),
            Constraint::Percentage(vertical),
            Constraint::Percentage((100 - vertical) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - horizontal) / 2),
            Constraint::Percentage(horizontal),
            Constraint::Percentage((100 - horizontal) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn visible_list_rows(area: Rect) -> usize {
    usize::from(area.height.saturating_sub(2)).max(1)
}

fn paged_window(cursor: usize, total_items: usize, visible_rows: usize) -> (usize, usize) {
    if total_items == 0 {
        return (0, 0);
    }

    let clamped_cursor = cursor.min(total_items.saturating_sub(1));
    let start = clamped_cursor.saturating_sub(visible_rows.saturating_sub(1));
    let end = (start + visible_rows).min(total_items);
    (start, end)
}

fn pagination_label(start: usize, end: usize, total: usize) -> String {
    if total == 0 {
        "(0 of 0)".to_string()
    } else {
        format!("({}-{} of {})", start + 1, end, total)
    }
}

fn toggle_all(values: &mut [bool]) {
    let should_enable_all = values.iter().any(|selected| !selected);
    for value in values {
        *value = should_enable_all;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paged_window_keeps_cursor_visible() {
        assert_eq!(paged_window(0, 25, 5), (0, 5));
        assert_eq!(paged_window(4, 25, 5), (0, 5));
        assert_eq!(paged_window(5, 25, 5), (1, 6));
        assert_eq!(paged_window(24, 25, 5), (20, 25));
    }

    #[test]
    fn pagination_label_uses_human_friendly_indices() {
        assert_eq!(pagination_label(0, 5, 25), "(1-5 of 25)");
        assert_eq!(pagination_label(20, 25, 25), "(21-25 of 25)");
        assert_eq!(pagination_label(0, 0, 0), "(0 of 0)");
    }

    #[test]
    fn toggle_all_enables_then_disables_everything() {
        let mut values = vec![true, false, true];
        toggle_all(&mut values);
        assert_eq!(values, vec![true, true, true]);

        toggle_all(&mut values);
        assert_eq!(values, vec![false, false, false]);
    }
}
